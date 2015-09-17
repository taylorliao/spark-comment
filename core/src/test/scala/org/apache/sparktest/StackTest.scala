package org.apache.sparktest
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map, Stack}
/**
 * Created by ASUS on 2015/8/20.
 */
object StackTest {
  def main (args: Array[String]) {
    val wait = new Stack[String]
        wait.push(new String("aaaa"))
        wait.push(new String("bbbb"))
    println(wait.pop());
    println(wait);
  }

}
