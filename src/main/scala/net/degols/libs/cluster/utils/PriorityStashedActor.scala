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
import scala.util.{Failure, Success, Try}

/**
  * Priority between [0;10[ is reserved for the PriorityStashedActor. Other messages should extend PriorityStashedMessage
  * @param priority
  */
@SerialVersionUID(0L)
abstract class PriorityStashedMessage(val priority: Int)

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

  /**
    * Execution time is in micro-seconds
    * @param executionTime
    */
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
abstract class PriorityStashedActor extends Actor with Logging {
  implicit val ec = context.system.dispatcher

  case class StashedElement(message: Any, sender: ActorRef, creationTime: Long)
  case class ExecuteElementNow(message: Any)

  private val actorStatistics: ActorStatistics = new ActorStatistics(self)

  /**
    * This id is only used to improve the traceging
    */
  protected var _id: String = "PriorityStashedActor"
  protected def id: String = _id
  protected def setId(newId: String): Unit = _id = newId

  protected val customStash: mutable.HashMap[Int, ListBuffer[StashedElement]] = new mutable.HashMap[Int, ListBuffer[StashedElement]]()

  /**
    * Key used is the message in itself. This is better than just using the hashCode() as the hashCode() could have
    * changed between the first reception of the message, and its processing. Obviously, this is not a good practice,
    * but at least we avoid strange behaviors with the PriorityStashedActor
   */
  protected val runningMessages: mutable.HashMap[Any, StashedElement] = new mutable.HashMap[Any, StashedElement]()

  protected val maximumRunningMessages: Int = 1

  /**
    * Wrapper around developer's code. Catch unexpected errors and avoid the PriorityStashed to be "stuck" because
    * of that
    * @param receive
    * @param message
    */
  private def callAroundReceive(receive: Receive, message: Any): Unit = {
    Try{
      super.aroundReceive(receive, message)
    } match {
      case Success(r) => r
      case Failure(e) =>
        error(s"$id: Uncaught error in the actor, this must never happen as the PriorityStashedActor could have been stuck!", e)
        endProcessing(message)
    }
  }

  override def aroundReceive(receive: Receive, message: Any): Unit = {
    message match {
      case _: GetActorStatistics =>
        sender() ! actorStatistics

      case x: ExecuteElementNow =>
        callAroundReceive(receive, x.message)

      case xWrapper: FinishedProcessing =>
        trace(s"$id: Received FinishedProcessing message for ${xWrapper.message}.")

        // Remove the message from the processing
        runningMessages.get(xWrapper.message) match {
          case Some(previousMessage) =>
            val diff = (System.nanoTime() - previousMessage.creationTime) / 1000L
            actorStatistics.addProcessedMessage(diff)
            runningMessages.remove(xWrapper.message)
          case None =>
            error(s"$id: Tried to remove message ${xWrapper.message} but we did not find it in the running messages: ${runningMessages.map(_.toString()).mkString(", ")}.")
        }

        // Check if we should process the next message, but according to their priority
        val orderedPriorities: List[Int] = customStash.keys.toList.sorted
        val remainingStash = orderedPriorities.map(priority => customStash(priority)).find(_.nonEmpty)

        remainingStash match {
          case Some(stash) =>
            val initialStashedElement: StashedElement = stash.remove(0)
            trace(s"$id: Remaining stash size to execute after removal: ${stash.size}. Total is ${customStash.map(_._2.size).sum}")

            // We create a new object as the time has changed. For performance purpose we could re-use the same object in the future
            val stashedElement = StashedElement(initialStashedElement.message, initialStashedElement.sender, System.nanoTime())
            runningMessages.put(initialStashedElement.message, stashedElement)

            // We cannot directly call aroundReceive here as the sender is invalid. We could override the sender(), but this
            // is not the best solution as context.sender() would still contain the old value
            self.tell(ExecuteElementNow(initialStashedElement.message), initialStashedElement.sender)
          case None =>
            //trace(s"$id: Empty stash, nothing more to process")
        }

      case x =>
        // Only send the order if there are less than x futures running at the same time
        val stashedElement = StashedElement(message, sender(), System.nanoTime())
        if(runningMessages.size < maximumRunningMessages) {
          trace(s"$id: Directly process message ${x} from ${sender()} to ${self}")
          runningMessages.put(message, stashedElement)

          // We directly received the message, no need to re-send an intermediate message, we can execute it directly with the correct sender() information
          callAroundReceive(receive, message)
        } else {
          //trace(s"$id: Waiting to process message ${message} from ${sender()} to ${self} as we have ${runningMessages.size} running messages.")
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
      future.andThen{
        case anyResult =>
          trace(s"$id: Finish processing (future) of ${message.hashCode()} at ${new DateTime()}")
          self ! FinishedProcessing(message)
      }
    } else {
      trace(s"$id: Finish processing (direct) of ${message.hashCode()} at ${new DateTime()}")
      self ! FinishedProcessing(message)
    }
  }
}