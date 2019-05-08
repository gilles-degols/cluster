package net.degols.libs.cluster.utils

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable}
import akka.dispatch.{PriorityGenerator, UnboundedStablePriorityMailbox}
import com.typesafe.config.Config
import org.bson.types.ObjectId
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Priority between [0;10[ is reserved for the PriorityStashedActor. Other messages should extend PriorityStashedMessage
  * @param priority
  */
@SerialVersionUID(0L)
abstract class PriorityStashedMessage(val priority: Int) {

  /**
    * We cannot rely on the default hashcode alone as we could have conflict. So we generate another object for each instance
    * @return
    */
  val oid: String = new ObjectId().toHexString
}

@SerialVersionUID(0L)
case class GarbageCollector() extends PriorityStashedMessage(5)

@SerialVersionUID(0L)
case class FinishedProcessing(message: Any) extends PriorityStashedMessage(5)

@SerialVersionUID(0L)
case class GetActorStatistics() extends PriorityStashedMessage(5)

/**
  * Handle the priority of messages not yet in the custom stash. As we won't always have a fast execution of messages
  * (maybe someone decided to do an Await or Thread.sleep somewhere), we cannot really guarantee the priority of messages
  * not yet received in the PriorityStashedActor. By implementing the UnboundedStablePriorityMailbox we solve this specific
  * use case.
  * You need to start the related actor extending PriorityStashedActor with "withMailbox(priority-stashed-actor)"
  */
final class PriorityStashedMailbox(settings: ActorSystem.Settings, config: Config) extends UnboundedStablePriorityMailbox(
  PriorityGenerator {
    case message: PriorityStashedMessage => message.priority
    case _ => Int.MaxValue
  }
)

/**
  * Statistics about the PipelineStepActor (messages received, etc.). Those statistics are quite basic, you should
  * use the Lightbend Telemetry or Kamon if you want more specific information.
  */
@SerialVersionUID(0L)
class ActorStatistics(val actorRef: ActorRef) {
  private var _lastMessageDateTime: DateTime = new DateTime()
  def lastMessageDateTime: DateTime = _lastMessageDateTime

  private var _totalProcessedMessages: Long = 0
  def totalProcessedMessage: Long = _totalProcessedMessages

  private var _averageProcessingTime: Double = 0.0
  def averageProcessingTime: Double = _averageProcessingTime

  def addProcessedMessage(executionTime: Long): Unit = {
    _averageProcessingTime = _averageProcessingTime + (executionTime - _averageProcessingTime)*1.0 / (_totalProcessedMessages + 1.0)
    _lastMessageDateTime = new DateTime()
    _totalProcessedMessages += 1
  }
}


/**
  * Implement an Actor with an home-made Stash with a (stable) priority queue. Its main goal is to provide a way to
  * have a service returning a future
  */
abstract class PriorityStashedActor extends Actor{
  implicit val ec = context.system.dispatcher

  case class StashedElement(message: Any, sender: ActorRef, creationTime: Long)
  case class ExecuteElementNow(message: Any)

  private val actorStatistics: ActorStatistics = new ActorStatistics(self)

  /**
    * This id is only used to improve the debugging
    */
  protected var _id: String = "PriorityStashedActor"
  protected def id: String = _id
  protected def setId(newId: String): Unit = _id = newId

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  protected val customStash: mutable.HashMap[Int, ListBuffer[StashedElement]] = new mutable.HashMap[Int, ListBuffer[StashedElement]]()
  protected val runningMessages: mutable.HashMap[String, StashedElement] = new mutable.HashMap[String, StashedElement]()

  protected val maximumRunningMessages: Int = 1

  /**
    * If the developer made some mistakes, we might have the actor stucked. If we did not receive a FinishedProcessing message
    * after maximumTimeProcessingMs, we assume a message was not correctly handled, so we remove it from the runningMessages.
    * This is only useful if the actor executes a future obviously.
    *
    * The garbage collector only works if there are waiting messages. Put 0L if you wish to deactivate it.
    */
  private var _maximumTimeProcessingMs: Long = 5 * 60 * 1000L
  private var _maximumTimeProcessingScheduler: Cancellable = _
  protected def maximumTimeProcessingMs: Long = _maximumTimeProcessingMs
  protected def setMaximumTimeProcessingMs(value: Long): Unit = {
    _maximumTimeProcessingMs = value
    if(_maximumTimeProcessingScheduler != null) {
      // Cancel the previous value
      _maximumTimeProcessingScheduler.cancel()
    }

    if(_maximumTimeProcessingMs > 0) {
      _maximumTimeProcessingScheduler = context.system.scheduler.schedule(_maximumTimeProcessingMs millis, _maximumTimeProcessingMs millis, self, GarbageCollector())
    } else {
      logger.info(s"$id: GarbageCollector is deactivated.")
    }
  }

  /**
    * As we can execute a message later on, but we still want to have a valid sender(), we need to manually override
    * the sender method. Using tell with a sender can lead to concurrency problems as we need to send the message to ourselves
    */

  /**
    * Initialize the garbage collector
    */
  setMaximumTimeProcessingMs(_maximumTimeProcessingMs)

  /**
    * Return the uid for a message. Might be composed of hashcode + unique id
    */
  private def uid(message: Any): String = {
    message match {
      case x: PriorityStashedMessage => s"${message.hashCode()}-${x.oid}"
      case x => s"${message.hashCode()}"
    }
  }

  override def aroundReceive(receive: Receive, message: Any): Unit = {
    message match {
      case x: GarbageCollector =>
        val messagesToRemove = runningMessages.filter(messageWrapper => {
          messageWrapper._2.creationTime < new DateTime().getMillis - _maximumTimeProcessingMs
        }).toList

        // We only remove running messages if there are waiting messages
        if(messagesToRemove.nonEmpty && customStash.values.map(_.size).sum > 0) {
          logger.error(s"$id: GarbageCollector must remove ${messagesToRemove.size} old messages. Please investigate or increase maximumTimeProcessingMs (value is ${maximumTimeProcessingMs} ms). Existing messages: ${runningMessages.values.size}")
          messagesToRemove.foreach(messageWrapper => {
           runningMessages.remove(messageWrapper._1)
          })
        }

      case x: GetActorStatistics =>
        sender() ! actorStatistics

      case x: ExecuteElementNow =>
        super.aroundReceive(receive, x.message)

      case xWrapper: FinishedProcessing =>
        logger.debug(s"$id: Received FinishedProcessing message.")

        // Remove the message from the processing
        runningMessages.get(uid(xWrapper.message)) match {
          case Some(previousMessage) =>
            val diff = new DateTime().getMillis - previousMessage.creationTime
            actorStatistics.addProcessedMessage(diff)
            runningMessages.remove(uid(xWrapper.message))
          case None =>
            logger.error(s"$id: Tried to remove message but we did not find it in the running messages: ${runningMessages.keys}.")
        }

        // Check if we should process the next message, but according to their priority
        val orderedPriorities: List[Int] = customStash.keys.toList.sorted
        val remainingStash = orderedPriorities.map(priority => customStash(priority)).find(_.nonEmpty)

        remainingStash match {
          case Some(stash) =>
            val message = stash.remove(0)
            logger.debug(s"$id: Remaining stash size to execute after removal: ${stash.size}. Total is ${customStash.map(_._2.size).sum}")

            // We create a new object as the time has changed. For performance purpose we could re-use the same object in the future
            val stashedElement = StashedElement(message.message, message.sender, new DateTime().getMillis)
            runningMessages.put(uid(message.message), stashedElement)

            // We cannot directly call aroundReceive here as the sender is invalid. We could override the sender(), but this
            // is not the best solution as context.sender() would still contain the old value
            self.tell(ExecuteElementNow(message.message), message.sender)
          case None =>
            logger.debug(s"$id: Empty stash, nothing more to process")
        }

      case x =>
        // Only send the order if there are less than x futures running at the same time
        val stashedElement = StashedElement(message, sender(), new DateTime().getMillis)
        if(runningMessages.size < maximumRunningMessages) {
          logger.debug(s"$id: Directly process message ${uid(x)} from ${sender()} to ${self}")
          runningMessages.put(uid(message), stashedElement)

          // We directly received the message, no need to re-send an intermediate message, we can execute it directly with the correct sender() information
          super.aroundReceive(receive, message)
        } else {
          logger.debug(s"$id: Waiting to process message ${uid(message)} from ${sender()} to ${self} as we have ${runningMessages.size} running messages.")
          val priority = message match {
            case fsMessage: PriorityStashedMessage => fsMessage.priority
            case otherMessage => Int.MaxValue
          }

          // Find the stash with the appropriate priority
          customStash.get(priority) match {
            case Some(stash) => stash.append(stashedElement)
            case None =>
              val stash = new ListBuffer[StashedElement]()
              stash.append(stashedElement)
              customStash.put(priority, stash)
          }
        }
    }
  }

  /**
    * Method to call when we have finished to process a message (in a future.map, or directly if no future is involved)
    * Never forget to call it, otherwise the actor could be stucked until the garbage collector kicks in.
    *
    * If we want to be sure to handle correctly the future, we can directly call this method with a future as parameter.
    * We will automatically handle the future failure
    * @param message
    */
  def endProcessing(message: Any, future: Future[Any] = null): Unit = {
    if(future != null) {
      future.transform{
        case Success(value) => Success(value)
        case Failure(exception) => Success("Failure")
      }.map(res => {
        val currentDate = new DateTime()
        logger.debug(s"$id: Finish processing of ${message.hashCode()} at $currentDate")
        self ! FinishedProcessing(message)
      })
    } else {
      val currentDate = new DateTime()
      logger.debug(s"$id: Finish processing of ${message.hashCode()} at $currentDate")
      self ! FinishedProcessing(message)
    }
  }
}