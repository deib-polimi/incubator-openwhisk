package org.apache.openwhisk.core.manager.monitoring

import akka.actor.{Actor, Props}
import org.apache.openwhisk.common.{AkkaLogging, TransactionId}
import org.apache.openwhisk.core.entity.ExecutableWhiskAction

import scala.collection.mutable

case class RequestArrival(action: ExecutableWhiskAction, tid: TransactionId)
case class ResponseArrival(action: ExecutableWhiskAction, tid: TransactionId)

class ResponseTimeMonitor
  extends Actor {

  var arrivalCount = mutable.Map.empty[ExecutableWhiskAction, Long]
  var transactionTime = mutable.Map.empty[TransactionId, Long]
  var aggregateRT = mutable.Map.empty[ExecutableWhiskAction, Double]

  implicit val logging = new AkkaLogging(context.system.log)

  def getArrivalCount(action: ExecutableWhiskAction): Option[Long] = arrivalCount.get(action)
  def getAggregateRT(action: ExecutableWhiskAction): Option[Double] = aggregateRT.get(action)

  def resetMonitor(action: ExecutableWhiskAction) = {
    arrivalCount -= action
    aggregateRT -= action
  }

  override def receive: Receive = {

    case rqMsg: RequestArrival =>
      handleRequestArrival(rqMsg)

    case rsMsg: ResponseArrival =>
      handleResponseArrival(rsMsg)
  }

  def handleRequestArrival(rqMsg : RequestArrival) : Unit = {
    arrivalCount.get(rqMsg.action) match {
      case Some(currentNRequests) =>
        arrivalCount(rqMsg.action) = currentNRequests + 1
      case None =>
        arrivalCount(rqMsg.action) = 1
    }
    val unixTime = System.currentTimeMillis
    transactionTime(rqMsg.tid) = unixTime
  }

  def handleResponseArrival(rtMsg : ResponseArrival) : Unit = {
    arrivalCount.get(rtMsg.action) match {
      case Some (currentCount) =>
        transactionTime.get(rtMsg.tid) match {
          case Some(timeOfRequest) =>
            val timeOfResponse = System.currentTimeMillis
            val responseTime = timeOfResponse - timeOfRequest
            aggregateRT.get(rtMsg.action) match {
              case Some(actualRT) =>
                var newRT = (actualRT * (currentCount - 1) + responseTime) / currentCount
                aggregateRT(rtMsg.action) = newRT
                transactionTime.remove(rtMsg.tid)
              case None =>
                aggregateRT(rtMsg.action) = responseTime
            }
          case None =>
            //TODO if all request arrivals are accounted for, there must be at least one transaction when a response time evt arrives
            logging.error(
              this,
              s"aggregated response time for ${rtMsg.action} can not be calculated because arrival count is None")
        }
      case None =>
        //TODO if all request arrivals are accounted for, there must be at least one accounted for when a response time evt arrives
        logging.error(
          this,
          s"aggregated response time for ${rtMsg.action} can not be calculated because arrival count is None")
    }
  }

}

object ResponseTimeMonitor{
  def props() = Props(new ResponseTimeMonitor)
}