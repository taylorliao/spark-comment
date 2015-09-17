/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.client

import java.util.concurrent.TimeoutException

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.remote.{AssociationErrorEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.{ActorLogReceive, RpcUtils, Utils, AkkaUtils}

/**
 * Interface allowing applications to speak with a Spark deploy cluster. Takes a master URL,
 * an app description, and a listener for cluster events, and calls back the listener when various
 * events occur.
 * AppClient是Application和Master交互的接口
 * 它负责了所有的与Master的交互
 * @param masterUrls Each url should look like spark://host:port.
 */
private[spark] class AppClient(
    actorSystem: ActorSystem,
    masterUrls: Array[String],
    appDescription: ApplicationDescription,
    listener: AppClientListener,
    conf: SparkConf)
  extends Logging {

  private val masterAkkaUrls = masterUrls.map(Master.toAkkaUrl(_, AkkaUtils.protocol(actorSystem)))

  private val REGISTRATION_TIMEOUT = 20.seconds
  private val REGISTRATION_RETRIES = 3

  private var masterAddress: Address = null
  private var actor: ActorRef = null
  private var appId: String = null
  private var registered = false
  private var activeMasterUrl: String = null

  private class ClientActor extends Actor with ActorLogReceive with Logging {
    var master: ActorSelection = null
    var alreadyDisconnected = false  // To avoid calling listener.disconnected() multiple times
    var alreadyDead = false  // To avoid calling listener.dead() multiple times
    var registrationRetryTimer: Option[Cancellable] = None

    override def preStart() {
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      try {
        registerWithMaster()
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          context.stop(self)
      }
    }

    def tryRegisterAllMasters() {
      for (masterAkkaUrl <- masterAkkaUrls) {
        logInfo("Connecting to master " + masterAkkaUrl + "...")
        val actor = context.actorSelection(masterAkkaUrl)
        actor ! RegisterApplication(appDescription)   // 向Master注册
      }
    }

    //actor首先向Master注册Application。如果超过20s没有接收到注册成功的消息，那么会重新注册；
    //如果重试超过3次仍未成功，那么本次提交就以失败结束了。
    def registerWithMaster() {
      tryRegisterAllMasters()
      import context.dispatcher
      var retries = 0
      registrationRetryTimer = Some {
        context.system.scheduler.schedule(REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT) {
          Utils.tryOrExit {
            retries += 1
            if (registered) {   // 注册成功，那么取消所有的重试
              registrationRetryTimer.foreach(_.cancel())
            } else if (retries >= REGISTRATION_RETRIES) {  // 重试超过指定次数（3次），则认为当前Cluster不可用，退出
              markDead("All masters are unresponsive! Giving up.")
            } else {
              tryRegisterAllMasters()  // 进行新一轮的重试
            }
          }
        }
      }
    }

    def changeMaster(url: String) {
      // activeMasterUrl is a valid Spark url since we receive it from master.
      activeMasterUrl = url
      master = context.actorSelection(
        Master.toAkkaUrl(activeMasterUrl, AkkaUtils.protocol(actorSystem)))
      masterAddress = Master.toAkkaAddress(activeMasterUrl, AkkaUtils.protocol(actorSystem))
    }

    private def isPossibleMaster(remoteUrl: Address) = {
      masterAkkaUrls.map(AddressFromURIString(_).hostPort).contains(remoteUrl.hostPort)
    }

    override def receiveWithLogging: PartialFunction[Any, Unit] = {
      case RegisteredApplication(appId_, masterUrl) =>    //  来自Master的注册Application成功的消息
        appId = appId_
        registered = true
        changeMaster(masterUrl)
        listener.connected(appId)

      case ApplicationRemoved(message) => //注：来自Master的删除Application的消息。Application执行成功或者失败最终都会被删除。
        markDead("Master removed our application: %s".format(message))
        context.stop(self)

      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>  //注：来自Master
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort,
          cores))
        master ! ExecutorStateChanged(appId, id, ExecutorState.RUNNING, None, None)
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      //注：来自Master的Executor状态更新的消息，如果是Executor是完成的状态，那么回调SchedulerBackend的executorRemoved的函数。
      case ExecutorUpdated(id, state, message, exitStatus) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus)
        }

      case MasterChanged(masterUrl, masterWebUiUrl) =>  //注：来自新竞选成功的Master
        logInfo("Master has changed, new master is at " + masterUrl)
        changeMaster(masterUrl)
        alreadyDisconnected = false
        sender ! MasterChangeAcknowledged(appId)

      case DisassociatedEvent(_, address, _) if address == masterAddress =>
        logWarning(s"Connection to $address failed; waiting for master to reconnect...")
        markDisconnected()

      case AssociationErrorEvent(cause, _, address, _, _) if isPossibleMaster(address) =>
        logWarning(s"Could not connect to $address: $cause")

      case StopAppClient =>  //注：来自AppClient::stop()
        markDead("Application has been stopped.")
        master ! UnregisterApplication(appId)
        sender ! true
        context.stop(self)
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    def markDead(reason: String) {
      if (!alreadyDead) {
        listener.dead(reason)
        alreadyDead = true
      }
    }

    override def postStop() {
      registrationRetryTimer.foreach(_.cancel())
    }

  }

  def start() {
    // Just launch an actor; it will call back into the listener.
    actor = actorSystem.actorOf(Props(new ClientActor))
  }

  def stop() {
    if (actor != null) {
      try {
        val timeout = RpcUtils.askTimeout(conf)
        val future = actor.ask(StopAppClient)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      actor = null
    }
  }
}
