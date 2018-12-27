package org.apache.openwhisk.core.manager.control


import akka.actor.{Actor, ActorRef, Props}
import org.apache.openwhisk.common.AkkaLogging
import org.apache.openwhisk.core.entity.ExecutableWhiskAction
import org.apache.openwhisk.core.manager.monitoring.{RTMetricsRequest, RTMetricsResponse}

import scala.collection.{immutable, mutable}

case class AllocationUpdate(allocation: immutable.Map[ExecutableWhiskAction, Float])
case class ControlPeriod()

class PlannersController(responseTimeMonitor: ActorRef,
                         ctnPool: ActorRef) extends Actor {


  private val planners: mutable.Map[ExecutableWhiskAction, Planner] = mutable.Map.empty[ExecutableWhiskAction, Planner]

  implicit val logging = new AkkaLogging(context.system.log)


  override def receive: Receive = {
    case ctMsg: ControlPeriod =>
      handleControlPeriod()
    case rtMsg: RTMetricsResponse =>
      handleRTMetrics(rtMsg.metrics)
    case allocationMsg: AllocationUpdate =>
      handleAllocation(allocationMsg.allocation)
  }

  protected def handleControlPeriod(): Unit = {
    responseTimeMonitor ! RTMetricsRequest()
  }

  private def handleRTMetrics(rtMetrics: immutable.Map[ExecutableWhiskAction, (Float,Long)]) : Unit = {
    logging.info(
      this,
      s"handling response time metrics response"
    )
    val allocation: immutable.Map[ExecutableWhiskAction, Float] = rtMetrics.map({
      case (k , (rt, req)) =>
        val planner = planners.getOrElseUpdate(k, new Planner())
        val nextAllocation = planner.nextResourceAllocation(rt, req)
        logging.info(
          this,
          s"next allocation for action ${k} with average rt ${rt} and count ${req} is ${nextAllocation}"
        )
        (k , nextAllocation)
    })

    ctnPool ! AllocationUpdate(allocation)
  }

  private def handleAllocation(allocation: immutable.Map[ExecutableWhiskAction, Float]) : Unit = {
    logging.info(
      this,
      s"handling allocation actuation response"
    )
    allocation.foreach({
      case (k , a) =>
        // should crash if planners(k) does not exist
        planners(k).updateState(a)
    })
  }
}

object PlannersController {
  def props(rtMonitor: ActorRef, ctnPool: ActorRef) =
    Props(new PlannersController(rtMonitor, ctnPool))
}

object Main {
  def main(args: Array[String]): Unit = {
  }
}
