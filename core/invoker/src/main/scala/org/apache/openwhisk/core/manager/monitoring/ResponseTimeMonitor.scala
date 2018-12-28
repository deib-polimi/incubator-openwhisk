package org.apache.openwhisk.core.manager.monitoring

import akka.actor.{Actor, ActorRef, Props}
import org.apache.openwhisk.common.{AkkaLogging, TransactionId}
import org.apache.openwhisk.core.entity.ExecutableWhiskAction

import scala.collection.{immutable, mutable}

case class RequestArrival(action: ExecutableWhiskAction, tid: TransactionId)
case class ResponseArrival(tid: TransactionId)
case class RTMetricsRequest()
case class RTMetricsResponse(metrics: immutable.Map[ExecutableWhiskAction,(Float,Long)])

class ResponseTimeMonitor
  extends Actor {

  var arrivalCount = mutable.Map.empty[ExecutableWhiskAction, Long]
  var transactionTime = mutable.Map.empty[TransactionId, (ExecutableWhiskAction, Long)]
  var aggregateRT = mutable.Map.empty[ExecutableWhiskAction, Float]

  implicit val logging = new AkkaLogging(context.system.log)

  override def receive: Receive = {
    case rqMsg: RequestArrival =>
      handleRequestArrival(rqMsg)

    case rsMsg: ResponseArrival =>
      handleResponseArrival(rsMsg)

    case rtMsg: RTMetricsRequest =>
      handleRTMetrics(sender)
  }

  def handleRTMetrics(sender: ActorRef) = {
    var metrics = immutable.Map.empty[ExecutableWhiskAction, (Float, Long)]
    logging.info(
      this,
      s"handling response time metrics request"
    )
    for((action, count) <- arrivalCount){
      getAggregateRT(action) match {
        case Some(rt) =>
          metrics += (action -> (rt, count))
        case None =>
          metrics += (action -> (0, count))
      }
    }
    resetMonitor()
    sender ! RTMetricsResponse(metrics)
  }

  def handleRequestArrival(rqMsg : RequestArrival) : Unit = {
    val timeOfArrival = System.currentTimeMillis
    logging.info(
      this,
      s"handling request arrival for action ${rqMsg.action})"
    )
    arrivalCount.getOrElseUpdate(rqMsg.action, 0)
    transactionTime(rqMsg.tid) = (rqMsg.action, timeOfArrival)
  }

  def handleResponseArrival(rtMsg : ResponseArrival) : Unit = {
    val timeOfResponseArrival = System.currentTimeMillis
    transactionTime.get(rtMsg.tid) match {
      case Some((action, timeOfRequestArrival)) =>
        arrivalCount.get(action) match {
          case Some (currentCount) =>
            val responseTime = (timeOfResponseArrival - timeOfRequestArrival).toFloat / 1000f //TODO try in seconds to check the CT behavior
            logging.info(
              this,
              s"handling response arrival for action ${action}), with RT = ${responseTime}ms"
            )
            aggregateRT.get(action) match {
              case Some(actualRT) =>
                var newRT = (actualRT * currentCount + responseTime) / (currentCount + 1)
                aggregateRT(action) = newRT
              case None =>
                aggregateRT(action) = responseTime
            }
            arrivalCount(action) = currentCount + 1
            transactionTime.remove(rtMsg.tid)
          case None =>
            //TODO if all request arrivals are accounted for, there must be at least the initialized zero when a response time evt arrives
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

  private def resetMonitor() = {
    for((action, count) <- arrivalCount){
      /*val pendingTransactions = transactionTime.filter {
        case (tId, (tAction, arrivalTime)) =>
          tAction.docid.equals(action.docid)
        case _ =>
          false
      }*/
      arrivalCount(action) = 0
      aggregateRT.remove(action)
    }
  }

  private def getArrivalCount(action: ExecutableWhiskAction): Option[Long] = arrivalCount.get(action)

  private def getAggregateRT(action: ExecutableWhiskAction): Option[Float] = aggregateRT.get(action)

}

object ResponseTimeMonitor{
  def props() = Props(new ResponseTimeMonitor)
}