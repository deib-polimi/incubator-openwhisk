package org.apache.openwhisk.core.manager.control

import scala.collection.immutable
import akka.actor.{Actor, ActorRef, Props}
import org.apache.openwhisk.core.entity.ExecutableWhiskAction

case class RTMetrics(metrics: immutable.Map[ExecutableWhiskAction,(Float,Long)])

class Planner(responseTimeMonitor: ActorRef)
  extends Actor {

  // to be read from conf
  private val MAX_CONTAINERS = 100f
  private val MIN_CONTAINERS = 1.0f
  private val SLA = 0.2f // set point

  private val A1_NOM = 0.1963f
  private val A2_NOM = 0.002f
  private val A3_NOM = 0.5658f
  private val P_NOM = 0.4f

  // the larger A (0,1) the smaller the proportional contribution
  // and the control reaches the steady-state slower but safer
  private val A = 0.9f

  // past integral contribution
  private var uiOld = 0.0f

  override def receive: Receive = {
    case rtMsg: RTMetrics =>
      handleRTMetrics(rtMsg.metrics)
  }

  def handleRTMetrics(rtMetrics: immutable.Map[ExecutableWhiskAction, (Float,Long)]) : Unit = {
    // read data from monitoring for each function
    for ((k, (rt, req)) <- rtMetrics) {
      val fCTNs = nextResourceAllocation(rt,req)
      
    }
  }

  def nextResourceAllocation(rt: Float, req: Long): Float = {

    val e = SLA - rt // error
    val ke = (A - 1) / (P_NOM - 1) * e // proportional contribution
    val ui = uiOld + (1 - P_NOM) * ke // integral contribution (starts from zero)
    val ut = ui + ke // PI contribution

    val core = req * (ut - A1_NOM - 1000.0f * A2_NOM) / (1000.0f * A3_NOM * (A1_NOM - ut))

    val approxCore: Float = Math.ceil(Math.min(MAX_CONTAINERS, Math.max(core, MIN_CONTAINERS))).toFloat // anti wind-up

    val approxUt = ((1000.0f * A2_NOM + A1_NOM) * req +
      1000.0f * A1_NOM * A3_NOM * approxCore) / (req + 1000.0f * A3_NOM * approxCore) // recompute PI contribution

    uiOld = approxUt - ke // update integral contribution

    approxCore // return cores
  }
}

object Planner{
  def props(rtMonitor: ActorRef) = Props(new Planner(rtMonitor))
}
