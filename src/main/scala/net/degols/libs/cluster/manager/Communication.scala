package net.degols.libs.cluster.manager

import akka.actor.ActorRef
import akka.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Random, Try}
import akka.pattern.ask
import scala.concurrent.duration._

case class RemoteReply(content: Any)

/**
  * Everything related to the communication between various workers
  */
class Communication(service: ClusterServiceLeader) {
  private val logger = LoggerFactory.getLogger(getClass)

  def actorRefsForId(workerTypeId: String): List[ActorRef] = {
    service._clusterTopology match {
      case None =>
        logger.error("ClusterTopology not yet available.")
        List.empty[ActorRef]
      case Some(topology) =>
        topology.getWorkerActors(workerTypeId).filter(_.isRunning).map(_.workerActorRef)
    }
  }

  def sendWithReply(sender: ActorRef, workerTypeId: String, message: Any)(implicit timeout: Timeout): Try[RemoteReply] = Try {
    var result: Try[RemoteReply] = Failure(new Exception("Method not called"))
    val actorRefs: List[ActorRef] = actorRefsForId(workerTypeId)

    if(actorRefs.nonEmpty) {
      // Simply take one at random. If we want to do smarter load balancing, you need to handle it yourselves
      val actorRef = Random.shuffle(actorRefs).head
      result = internalSendWithReply(sender, actorRef, message)
      if(result.isSuccess) {
        return result
      }
    }

    result.get
  }

  def sendWithReply(sender: ActorRef, actorRef: ActorRef, message: Any)(implicit timeout: Timeout): Try[RemoteReply] = Try{
    var result: Try[RemoteReply] = Failure(new Exception("Method not called"))
    result = internalSendWithReply(sender, actorRef, message)
    if(result.isSuccess) {
      return result
    }

    result.get
  }

  def askForReply(sender: ActorRef, actorRef: ActorRef, message: Any)(implicit ec: ExecutionContext, timeout: Timeout): Future[RemoteReply] = {
    implicit val send: ActorRef = sender
    actorRef.ask(message)(timeout).map(raw => {
      RemoteReply(raw)
    })
  }

  def sendWithoutReply(sender: ActorRef, actorRef: ActorRef, message: Any): Try[Unit] = Try{
    actorRef.tell(message, sender)
  }

  private def internalSendWithReply(sender: ActorRef, actorRef: ActorRef, message: Any)(implicit timeout: Timeout): Try[RemoteReply] = Try{
    try{
      implicit val send = sender
      Await.result(actorRef.ask(message), timeout.duration) match {
        case x: Throwable =>
          logger.error(s"Got a generic Throwable exception (not expected) while sending a message to $actorRef: $x")
          throw x
        case x =>
          logger.debug(s"Received result from $actorRef : $x")
          RemoteReply(content=x)
      }
    } catch {
      case x: TimeoutException =>
        logger.warn(s"Got TimeoutException while trying to send a message ($message) to $actorRef.")
        throw x
      case x: Throwable =>
        logger.error(s"Got a generic Throwable exception (not expected) while sending a message to $actorRef.")
        throw x
    }
  }
}

object Communication {
  /**
    * Format a given actor name to have its full path with component / package
    * @param component
    * @param packag
    * @param name
    * @return
    */
  def fullActorName(component: String, packag: String, name: String): String = {
    s"$component:$packag:$name"
  }
}