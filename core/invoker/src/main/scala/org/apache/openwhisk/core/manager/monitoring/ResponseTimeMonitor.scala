package org.apache.openwhisk.core.manager.monitoring

import akka.actor.{Actor, Props}
import org.apache.openwhisk.common.{AkkaLogging, TransactionId}
import org.apache.openwhisk.core.entity.ExecutableWhiskAction

import scala.collection.mutable

case class RequestArrival(action: ExecutableWhiskAction, tid: TransactionId)
case class ResponseArrival(tid: TransactionId)
case class RequestArrivalCount(action: ExecutableWhiskAction, count: Option[Long])
case class ResponseTimeAverage(action: ExecutableWhiskAction, average: Option[Double])

class ResponseTimeMonitor
  extends Actor {

  var arrivalCount = mutable.Map.empty[ExecutableWhiskAction, Long]
  var transactionTime = mutable.Map.empty[TransactionId, (ExecutableWhiskAction, Long)]
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

    case raMsg: RequestArrivalCount =>
      sender ! RequestArrivalCount(raMsg.action, getArrivalCount(raMsg.action))

    case rtMsg: ResponseTimeAverage =>
      sender ! ResponseTimeAverage(rtMsg.action, getAggregateRT(rtMsg.action))
  }

  def handleRequestArrival(rqMsg : RequestArrival) : Unit = {
    val timeOfArrival = System.currentTimeMillis
    logging.info(
      this,
      s"handling request arrival for action ${rqMsg.action})"
    )
    arrivalCount.get(rqMsg.action) match {
      case Some(currentNRequests) =>
        arrivalCount(rqMsg.action) = currentNRequests + 1
      case None =>
        arrivalCount(rqMsg.action) = 1
    }
    transactionTime(rqMsg.tid) = (rqMsg.action, timeOfArrival)
  }

  def handleResponseArrival(rtMsg : ResponseArrival) : Unit = {
    val timeOfResponseArrival = System.currentTimeMillis
    transactionTime.get(rtMsg.tid) match {
      case Some((action, timeOfRequestArrival)) =>
        logging.info(
          this,
          s"handling response arrival for action ${action})"
        )
        arrivalCount.get(action) match {
          case Some (currentCount) =>
            val responseTime = timeOfResponseArrival - timeOfRequestArrival
            aggregateRT.get(action) match {
              case Some(actualRT) =>
                var newRT = (actualRT * (currentCount - 1) + responseTime) / currentCount
                aggregateRT(action) = newRT
              case None =>
                aggregateRT(action) = responseTime
            }
            transactionTime.remove(rtMsg.tid)
          case None =>
            //TODO if all request arrivals are accounted for, there must be at least one accounted for when a response time evt arrives
            logging.error(
              this,
              s"aggregated response time for ${action} can not be calculated because arrival count is None")
            transactionTime.remove(rtMsg.tid)
        }
      case None =>
        //TODO if all request arrivals are accounted for, there must be at least one transaction when a response time evt arrives
        logging.error(
          this,
          s"aggregated response time for transaction ${rtMsg.tid} can not be calculated because transaction time is None")
    }
  }

}

object ResponseTimeMonitor{
  def props() = Props(new ResponseTimeMonitor)
}