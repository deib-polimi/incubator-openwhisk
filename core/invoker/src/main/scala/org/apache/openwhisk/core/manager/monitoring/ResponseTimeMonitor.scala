package org.apache.openwhisk.core.manager.monitoring

import akka.actor.Actor
import org.apache.openwhisk.common.AkkaLogging
import org.apache.openwhisk.core.entity.ExecutableWhiskAction

import scala.collection.immutable

class ResponseTimeMonitor
  extends Actor {

  case class RequestArrival(action: ExecutableWhiskAction)
  case class ResponseTime(action: ExecutableWhiskAction, rt: Double)


  var aggregateAR = immutable.Map.empty[ExecutableWhiskAction, Long]
  var aggregateRT = immutable.Map.empty[ExecutableWhiskAction, Double]

  implicit val logging = new AkkaLogging(context.system.log)

  override def receive: Receive = {

    case raMsg: RequestArrival =>
      aggregateAR.get(raMsg.action) match {
        case Some(currentNRequests) =>
          aggregateAR(raMsg.action) = currentNRequests + 1
        case None =>
          aggregateAR + (raMsg.action -> 1)
      }

    case rtMsg: ResponseTime =>
      aggregateRT.get(rtMsg.action) match {
        case Some(actualRT) =>
          var newRT = actualRT + rtMsg.rt
          aggregateRT(rtMsg.action) = newRT
        case None =>
          aggregateRT + (rtMsg.action -> rtMsg.rt)
      }
  }


}
