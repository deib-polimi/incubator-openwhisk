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

  def getAggregateAR(action: ExecutableWhiskAction): Option[Long] = aggregateAR.get(action)
  def getAggregateRT(action: ExecutableWhiskAction): Option[Double] = aggregateRT.get(action)

  override def receive: Receive = {

    case raMsg: RequestArrival =>
      aggregateAR.get(raMsg.action) match {
        case Some(currentNRequests) =>
          aggregateAR(raMsg.action) = currentNRequests + 1
        case None =>
          aggregateAR + (raMsg.action -> 1)
      }

    case rtMsg: ResponseTime =>
      aggregateAR.get(rtMsg.action) match {
        case Some (currentNRequests) =>
          aggregateRT.get (rtMsg.action) match {
            case Some(actualRT) =>
              var newRT = (actualRT * (currentNRequests - 1) + rtMsg.rt) / currentNRequests
              aggregateRT(rtMsg.action) = newRT
            case None =>
              aggregateRT + (rtMsg.action -> rtMsg.rt)
          }
        case None =>
          //TODO if all request arrivals are logged, there must be at least one when a response time msg arrives
      }
  }


}
