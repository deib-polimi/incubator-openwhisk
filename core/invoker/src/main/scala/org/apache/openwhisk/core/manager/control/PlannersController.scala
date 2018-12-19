package org.apache.openwhisk.core.manager.control


import java.util.{Timer, TimerTask}

import scala.collection.{immutable, mutable}
import akka.actor.{Actor, ActorRef, Props}
import org.apache.openwhisk.core.entity.ExecutableWhiskAction

case class RTMetrics(metrics: immutable.Map[ExecutableWhiskAction,(Float,Long)])
case class RTMetricsRequest()

case class AllocationUpdate(allocation: immutable.Map[ExecutableWhiskAction, Float])

class PlannersController(controlPeriod: Int, // in milliseconds
                         responseTimeMonitor: ActorRef,
                         ctnPool: ActorRef) extends Actor {

  val planners: mutable.Map[ExecutableWhiskAction, Planner] = mutable.Map.empty[ExecutableWhiskAction, Planner]
  val timer = new Timer();

  protected def tick(): Unit = {
    // at each control period ask RTMetrics
    responseTimeMonitor ! RTMetricsRequest()
  }

  def start(): Unit = {

    val task = new TimerTask {
      def run() = tick()
    }

    timer.scheduleAtFixedRate(task, controlPeriod, controlPeriod)
  }

  def stop(): Unit = {
    timer.cancel()
  }

  override def receive: Receive = {
    case rtMsg: RTMetrics =>
      handleRTMetrics(rtMsg.metrics)
  }

  private def handleRTMetrics(rtMetrics: immutable.Map[ExecutableWhiskAction, (Float,Long)]) : Unit = {

    val allocation: Map[ExecutableWhiskAction, Float] = rtMetrics.map({
      case (k , (rt, req)) =>
        val planner = planners.getOrElseUpdate(k, new Planner())
        (k ,planner.nextResourceAllocation(rt, req))
    })

    ctnPool ! AllocationUpdate(allocation)
  }

}

object PlannersController {
  def props(controlPeriod: Int, rtMonitor: ActorRef, ctnPool: ActorRef) =
    Props(new PlannersController(controlPeriod, rtMonitor, ctnPool))
}

object Main {
  def main(args: Array[String]): Unit = {

  }
}
