package org.apache.openwhisk.core.manager.control


import java.util.{Timer, TimerTask}

import scala.collection.{immutable, mutable}
import akka.actor.{Actor, ActorRef, Props}
import org.apache.openwhisk.core.entity.ExecutableWhiskAction
import org.apache.openwhisk.core.manager.monitoring.{RTMetrics}

case class AllocationUpdate(allocation: immutable.Map[ExecutableWhiskAction, Float])

class PlannersController(controlPeriod: Int, // in milliseconds
                         responseTimeMonitor: ActorRef,
                         ctnPool: ActorRef) extends Actor {


  private val planners: mutable.Map[ExecutableWhiskAction, Planner] = mutable.Map.empty[ExecutableWhiskAction, Planner]
  private val timer = new Timer();
  private var started = false

  protected def tick(): Unit = {
    // at each control period ask for RTMetrics
    responseTimeMonitor ! RTMetrics
  }

  def start(): Unit = synchronized {
    if (!started) {
      val task = new TimerTask {
        def run() = tick()
      }
      timer.scheduleAtFixedRate(task, controlPeriod, controlPeriod)
      started = true
    }
  }

  def stop(): Unit = synchronized {
    if (started) {
      started = false
      timer.cancel()
    }
  }

  override def receive: Receive = {
    case rtMsg: RTMetrics =>
      handleRTMetrics(rtMsg.metrics)
    case allocationMsg: AllocationUpdate =>
      handleAllocation(allocationMsg.allocation)
  }

  private def handleRTMetrics(rtMetrics: immutable.Map[ExecutableWhiskAction, (Float,Long)]) : Unit = {

    val allocation: immutable.Map[ExecutableWhiskAction, Float] = rtMetrics.map({
      case (k , (rt, req)) =>
        val planner = planners.getOrElseUpdate(k, new Planner())
        (k ,planner.nextResourceAllocation(rt, req))
    })

    ctnPool ! AllocationUpdate(allocation)
  }

  private def handleAllocation(allocation: immutable.Map[ExecutableWhiskAction, Float]) : Unit = {
    allocation.foreach({
      case (k , a) =>
        // should crash if planners(k) does not exist
        planners(k).updateState(a)
    })
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
