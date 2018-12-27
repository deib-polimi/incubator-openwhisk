package org.apache.openwhisk.core.manager.control

class Planner {

  //TODO: to be read from conf
  private val MAX_CONTAINERS = 2f
  private val MIN_CONTAINERS = 0f
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
  private var req = 0.0f
  private var ke = 0.0f

  def nextResourceAllocation(rt: Float, req: Long): Float = {

    if (rt > 0 || req > 0) {

      val e = SLA - rt // error

      this.req = req
      this.ke = (A - 1) / (P_NOM - 1) * e // proportional contribution

      val ui = uiOld + (1 - P_NOM) * ke // integral contribution (starts from zero)
      val ut = ui + ke // PI contribution

      val core = req * (ut - A1_NOM - 1000.0f * A2_NOM) / (1000.0f * A3_NOM * (A1_NOM - ut))

      val approxCore: Float = Math.ceil(Math.min(MAX_CONTAINERS, Math.max(core, MIN_CONTAINERS))).toFloat // anti wind-up

      approxCore // return cores
    }
    else 0.0f
  }

  def updateState(allocatedCore: Float): Unit = {

    val approxUt = ((1000.0f * A2_NOM + A1_NOM) * req +
      1000.0f * A1_NOM * A3_NOM * allocatedCore) / (req + 1000.0f * A3_NOM * allocatedCore) // recompute PI contribution

    uiOld = approxUt - ke // update integral contribution
  }
}

