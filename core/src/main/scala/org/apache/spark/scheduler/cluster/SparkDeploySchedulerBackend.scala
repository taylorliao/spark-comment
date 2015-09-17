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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.Semaphore

import org.apache.spark.rpc.RpcAddress
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{AppClient, AppClientListener}
import org.apache.spark.scheduler.{ExecutorExited, ExecutorLossReason, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.util.Utils
//SchedulerBackend调度和管理任务。它包含资源分配和任务调度。它实现了FIFO调度和FAIR调度，
//基于此来决定不同jobs之间的调度顺序。并且管理任务，包括任务的提交和终止，为饥饿任务启动备份任务.
//通过创建AppClient，可以向Standalone的Master注册Application，
//然后Master会通过Application的信息为它分配Worker，包括每个worker上使用CPU core的数目等
//SparkDeploySchedulerBackend装备好启动Executor的必要参数后，创建AppClient，并通过一些回调函数来得到Executor和连接等信息；
//通过org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend.DriverActor与ExecutorBackend来进行通信
private[spark] class SparkDeploySchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with AppClientListener
  with Logging {

  private var client: AppClient = null   //Application与Master的接口
  private var stopping = false

  @volatile var shutdownCallback: SparkDeploySchedulerBackend => Unit = _
  @volatile private var appId: String = _

  private val registrationBarrier = new Semaphore(0)
  //注：获得每个executor最多的CPU core数目
  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)
  private val totalExpectedCores = maxCores.getOrElse(0)

  override def start() {
    super.start()

    // The endpoint for executors to talk to us
    val driverUrl = rpcEnv.uriOf(SparkEnv.driverActorSystemName,
      RpcAddress(sc.conf.get("spark.driver.host"), sc.conf.get("spark.driver.port").toInt),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    //注：现在executor还没有申请，因此关于executor的所有信息都是未知的。
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
    //注：设置executor运行时需要的环境变量
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val libraryPathEntries = sc.conf.getOption("spark.executor.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath =
      if (sys.props.contains("spark.testing")) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    //注：在Worker上通过org.apache.spark.deploy.worker.ExecutorRunner启动
    // org.apache.spark.executor.CoarseGrainedExecutorBackend，这里准备启动它需要的参数
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")
    val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
    val appDesc = new ApplicationDescription(sc.appName, maxCores, sc.executorMemory,
      command, appUIAddress, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor)
    client = new AppClient(sc.env.actorSystem, masters, appDesc, this, conf)
    client.start()
    //注：在Master返回注册Application成功的消息后，AppClient会回调本class的connected，完成了Application的注册
    waitForRegistration()   //？？ 会进行阻塞么，只到appclient.actor 有返回值。
  }

  override def stop() {
    stopping = true
    super.stop()
    client.stop()

    val callback = shutdownCallback
    if (callback != null) {
      callback(this)
    }
  }

  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    this.appId = appId
    notifyContext()
}

  override def disconnected() {
    notifyContext()
    if (!stopping) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String) {
    notifyContext()
    if (!stopping) {
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stop()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d cores, %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(fullId: String, message: String, exitStatus: Option[Int]) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code)
      case None => SlaveLost(message)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason.toString)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  private def waitForRegistration() = {
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    registrationBarrier.release()
  }

}
