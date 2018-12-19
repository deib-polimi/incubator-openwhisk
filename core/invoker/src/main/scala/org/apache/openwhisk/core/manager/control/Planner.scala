package org.apache.openwhisk.core.manager.control

import akka.actor.Actor
import org.apache.openwhisk.core.entity.ExecManifest.{ImageName, RuntimeManifest}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.manager.monitoring.ResponseTimeMonitor

class Planner(action: ExecutableWhiskAction) extends Actor {

  // contains the aggregated metrics (request arrival count, response time)
  // at each control iteration, the method reset(action) should be called
  private val RTMonitor = new ResponseTimeMonitor()

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
    case _ => ()
  }

  def nextResourceAllocation(): Float = {

    // read data from monitoring
    val rt: Float = RTMonitor.getAggregateRT(action).map(_.toFloat).getOrElse(0f); // response time
    val req: Float = RTMonitor.getArrivalCount(action).map(_.toFloat).getOrElse(0f); // number of request
    RTMonitor.resetMonitor(action);

    if (rt > 0 || req > 0) {
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
    else 0

  }

}

object Main {

  def main(args: Array[String]): Unit = {
    val exec = CodeExecAsString(RuntimeManifest("actionKind", ImageName("testImage")), "testCode", None)
    val invocationNamespace = EntityName("invocationSpace")
    val action = ExecutableWhiskAction(EntityPath("actionSpace"), EntityName("actionName"), exec)
    val p = new Planner(action)
    p.nextResourceAllocation()
  }
}