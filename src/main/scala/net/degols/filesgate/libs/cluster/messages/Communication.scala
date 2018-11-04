package net.degols.filesgate.libs.cluster.messages

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}
import scala.util.{Failure, Random, Try}

case class RemoteReply(content: Any)

object Communication {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private var _clusterTopology: Option[ClusterTopology] = None

  private var _askTimeoutSecond: Int = 0

  def setAskTimeoutSecond(value: Int): Unit = if(value > 0) _askTimeoutSecond = value else throw new Exception("Value out of range")

  /**
    * Number of retries to execute if a message could not be sent
    */
  private var _maxRetries: Int = 10

  def setMaxRetries(value: Int): Unit = if(value > 0) _maxRetries = value else throw new Exception("Value out of range")

  def setClusterTopology(clusterTopology: ClusterTopology): Unit = _clusterTopology = Option(clusterTopology)

  def sendWithReply(sender: ActorRef, workerTypeId: String, message: Any): Try[RemoteReply] = Try {
    var attempt = 0
    var result: Try[RemoteReply] = Failure(new Exception("Method not called"))
    while(attempt < _maxRetries) {
      val actorRefs: List[ActorRef] = _clusterTopology match {
        case None =>
          logger.error("ClusterTopology not yet available.")
          List.empty[ActorRef]
        case Some(topology) =>
          topology.getWorkerActors(workerTypeId).filter(_.isRunning).map(_.workerActorRef)
      }

      if(actorRefs.nonEmpty) {
        // Simply take one at random. If we want to do smarter load balancing, you need to handle it yourselves
        val actorRef = Random.shuffle(actorRefs).head
        result = internalSendWithReply(sender, actorRef, message, _askTimeoutSecond)
        if(result.isSuccess) {
          return result
        }
      }

      attempt += 1

      if(attempt < _maxRetries) {
        Thread.sleep(500L)
      }
    }

    result.get
  }

  def sendWithReply(sender: ActorRef, actorRef: ActorRef, message: Any): Try[RemoteReply] = Try{
    var attempt = 0
    var result: Try[RemoteReply] = Failure(new Exception("Method not called"))
    while(attempt < _maxRetries) {
      result = internalSendWithReply(sender, actorRef, message, _askTimeoutSecond)
      if(result.isSuccess) {
        return result
      }
      attempt += 1

      if(attempt < _maxRetries) {
        Thread.sleep(500L)
      }
    }

    result.get
  }

  def sendWithoutReply(sender: ActorRef, actorRef: ActorRef, message: Any): Try[Unit] = Try{
    actorRef.tell(message, sender)
  }

  private def internalSendWithReply(sender: ActorRef, actorRef: ActorRef, message: Any, timeoutSecond: Int): Try[RemoteReply] = Try{
    try{
      implicit val timeout: Timeout = timeoutSecond second ;
      Await.result(actorRef.ask(message, sender), timeoutSecond second) match {
        case x: Throwable =>
          logger.error(s"Got a generic Throwable exception (not expected) while sending a message to $actorRef: $x")
          throw x
        case x =>
          logger.debug(s"Received result from $actorRef : $x")
          RemoteReply(content=x)
      }
    } catch {
      case x: TimeoutException =>
        logger.warn(s"Got TimeoutException while trying to send a message to $actorRef.")
        throw x
      case x: Throwable =>
        logger.error(s"Got a generic Throwable exception (not expected) while sending a message to $actorRef.")
        throw x
    }
  }
}
