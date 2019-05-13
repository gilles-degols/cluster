package net.degols.libs.cluster.manager

import akka.actor.{ActorContext, ActorRef}
import akka.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Random, Success, Try}
import akka.pattern.ask
import net.degols.libs.cluster.messages.{ClusterRemoteMessage, GetActorRefsFor, GetAllWorkerTypeIds, GetInfoFromActorRef, InfoFromActorRef, MessageWasHandled, MissingActor, UnrespondingManager, WorkerActorHealth, WorkerTypeOrder}

import scala.concurrent.duration._

case class RemoteReply(content: Any)

/**
  * Everything related to the communication between various workers
  */
class Communication(service: ClusterServiceLeader) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Return information about an actorRef. Typically useful to find our own orderId
    */
  def infoFromActorRef(targetActorRef: ActorRef)(implicit context: ActorContext): Future[Option[InfoFromActorRef]] = {
    implicit val sender = context.self
    implicit val ac = context.dispatcher
    val m = GetInfoFromActorRef(sender, targetActorRef)

    service.askClusterInfo(m)
      .transform{
        case Success(r) =>
          Success(r)
        case Failure(e) =>
          logger.error("Problem while fetching data from the manager", e)
          Success(None)
      }.map(res => {
      res.flatMap(_.asInstanceOf[Option[InfoFromActorRef]])
    })
  }

  /**
    * ActorRefs matching the given WorkerTypeId and the OrderId
    */
  def actorRefsForTypeAndOrder(workerTypeId: String, orderId: String)(implicit context: ActorContext): Future[Seq[ActorRef]] = {
    implicit val sender = context.self
    implicit val ac = context.dispatcher
    val m = GetActorRefsFor(sender, workerTypeId, Option(orderId), None)

    service.askClusterInfo(m)
      .transform{
        case Success(r) =>
          Success(r)
        case Failure(e) =>
          logger.error("Problem while fetching data from the manager", e)
          Success(None)
      }.map(res => {
      res.map(_.asInstanceOf[List[ActorRef]]).getOrElse(List.empty[ActorRef])
    })
  }

  /**
    * List of all available workerTypeIds
    * TODO: This might not be useful anymore
    * @return
    */
  def workerTypeIds()(implicit context: ActorContext): Future[Seq[String]] = {
    implicit val sender = context.self
    implicit val ac = context.dispatcher
    val m = GetAllWorkerTypeIds(sender)

    service.askClusterInfo(m)
      .transform{
        case Success(r) =>
          Success(r)
        case Failure(e) =>
          logger.error("Problem while fetching data from the manager", e)
          Success(None)
      }.map(res => {
      res.map(_.asInstanceOf[List[String]]).getOrElse(List.empty[String])
    })
  }

  /**
    * Convert a local WorkerOrder to a WorkerTypeOrder to send it to the Manager
    * @param workerOrder
    */
  private[manager] def convertWorkerOrder(workerOrder: WorkerOrder)(implicit context: ActorContext): WorkerTypeOrder = {
    val orderId = workerOrder.id match {
      case Some(id) =>
        if(!id.contains(workerOrder.fullName)) {
          throw new Exception(s"Invalid custom orderId '$id' for a WorkerOrder ${workerOrder.fullName}, it MUST always contain the fullName to avoid " +
            "clashes.")
        }
        logger.debug(s"The developer specified himself/herself a specific id '$id' for a WorkerOrder: ${workerOrder.fullName}, be careful.")
        id
      case None =>
        // The orderId must remain the same across the different JVM, so it should not be customized with node information
        // Unless the developer really knows what he is doing
        s"${workerOrder.fullName}_default"
    }
    WorkerTypeOrder(context.self, workerOrder.fullName, workerOrder.balancerType, orderId, workerOrder.metadata.toString())
  }

  /**
    * Send a WorkerOrder to the manager (if it exists)
    * @return successfull future if it's a success
    */
  def sendWorkerOrder(workerOrder: WorkerOrder)(implicit context: ActorContext): Future[Unit.type] = {
    sendInfoToManager(convertWorkerOrder(workerOrder))
  }

  /**
    * Send an arbitrary message to the Manager, and wait for its acknowledgement. We do not care about the result on the
    * other-side, just that it sent something back
    * @param managerActorRef optional ActorRef of the Manager that we need to use (instead of the default local manager) to contact it
    * @return
    */
  def sendInfoToManager(message: ClusterRemoteMessage, managerActorRef: Option[ActorRef] = None)(implicit context: ActorContext): Future[Unit.type] = {
    implicit val ac = context.dispatcher
    val manager = if(managerActorRef.isDefined) managerActorRef
    else service.manager
    manager match {
      case Some(actorRef) =>
        logger.debug(s"Sending message $message to Manager")
        implicit val timeout: Timeout = Timeout(10 seconds)

        (actorRef ? message).map(result => {
          Unit // We always expect the MessageWasHandled, so as long as we have a successful future, that's all that matter
        })
      case None =>
        logger.warn(s"There is no manager available for the moment to send the message $message!")
        Future{throw new UnrespondingManager("No manager available for the moment")}
    }
  }

  def actorRefsForId(workerTypeId: String)(implicit context: ActorContext): Future[Seq[ActorRef]] = {
    implicit val sender = context.self
    implicit val ac = context.dispatcher
    val m = GetActorRefsFor(sender, workerTypeId, None, Option(true))

    service.askClusterInfo(m)
      .transform{
        case Success(r) =>
          Success(r)
        case Failure(e) =>
          logger.error("Problem while fetching data from the manager", e)
          Success(None)
      }.map(res => {
      res.map(_.asInstanceOf[List[ActorRef]]).getOrElse(List.empty[ActorRef])
    })
  }

  def sendWithReply(workerTypeId: String, message: Any)(implicit timeout: Timeout, context: ActorContext): Future[RemoteReply] = {
    implicit val ac = context.dispatcher
    actorRefsForId(workerTypeId)
      .flatMap(actorRefs => {
        if(actorRefs.nonEmpty) {
          // Simply take one at random. If we want to do smarter load balancing, you need to handle it yourselves
          val actorRef = Random.shuffle(actorRefs).head
          sendWithReply(actorRef, message)
        } else {
          Future{throw new MissingActor(s"Not actor found for $workerTypeId")}
        }
      })
  }

  def sendWithReply(actorRef: ActorRef, message: Any)(implicit timeout: Timeout, context: ActorContext): Future[RemoteReply] = {
    implicit val ac = context.dispatcher
    actorRef.ask(message)(timeout.duration).map(raw => RemoteReply(raw))
  }

  def sendWithoutReply(actorRef: ActorRef, message: Any)(implicit context: ActorContext): Try[Unit] = Try{
    implicit val sender: ActorRef = context.self
    actorRef.tell(message, sender)
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