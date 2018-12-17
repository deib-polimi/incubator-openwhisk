package org.apache.openwhisk.core.manager.monitoring

import akka.actor.Actor
import org.apache.openwhisk.common.AkkaLogging
import org.apache.openwhisk.core.entity.ExecutableWhiskAction

import scala.collection.immutable

class ResponseTimeMonitor
  extends Actor {

  case class RequestArrival(action: ExecutableWhiskAction)
  case class ResponseTime(action: ExecutableWhiskAction, rt: Double)


  var arrivalCount = immutable.Map.empty[ExecutableWhiskAction, Long]
  var aggregateRT = immutable.Map.empty[ExecutableWhiskAction, Double]

  implicit val logging = new AkkaLogging(context.system.log)

  def getArrivalCount(action: ExecutableWhiskAction): Option[Long] = arrivalCount.get(action)
  def getAggregateRT(action: ExecutableWhiskAction): Option[Double] = aggregateRT.get(action)

  def resetMonitor(action: ExecutableWhiskAction) = {
    arrivalCount -= action
    aggregateRT -= action
  }

  override def receive: Receive = {

    case raMsg: RequestArrival =>
      arrivalCount.get(raMsg.action) match {
        case Some(currentNRequests) =>
          arrivalCount += (raMsg.action -> (currentNRequests + 1))
        case None =>
          arrivalCount += (raMsg.action -> 1)
      }

    case rtMsg: ResponseTime =>
      arrivalCount.get(rtMsg.action) match {
        case Some (currentCount) =>
          aggregateRT.get (rtMsg.action) match {
            case Some(actualRT) =>
              var newRT = (actualRT * (currentCount - 1) + rtMsg.rt) / currentCount
              aggregateRT += (rtMsg.action -> newRT)
            case None =>
              aggregateRT += (rtMsg.action -> rtMsg.rt)
          }
        case None =>
          //TODO if all request arrivals are accounted for, there must be at least one when a response time msg arrives
          logging.error(
            this,
            s"aggregated response time for ${rtMsg.action} can not be calculated because arrival count is None")
      }
  }
}

object ResponseTimeMonitor