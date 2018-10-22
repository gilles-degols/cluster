package net.degols.filesgate.libs.cluster.manager

import akka.actor.{Actor, ActorRef, Terminated}
import com.google.inject.{Inject, Singleton}
import net.degols.filesgate.libs.cluster.messages._
import net.degols.filesgate.libs.election.{IAmFollower, IAmLeader, TheLeaderIs}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Manage the various Workers in the current JVM, only one instance is allowed, hence the Singleton. This is the actor
  * knowing which WorkerActors are available, and how to start them.
  */
@Singleton
abstract class WorkerLeader @Inject()(localManager: Manager) extends Actor{
  private val logger = LoggerFactory.getLogger(getClass)

  override def preStart(): Unit = {
    super.preStart()
    // Inform the localManager of our existence
    localManager.self ! IAmTheWorkerLeader
  }

  /**
    * Current Manager in charge of the cluster (it can be different than the localManager obviously)
    */
  var manager: Option[ActorRef] = None

  /**
    * Information about the current topology we have in this jvm
    */
  lazy val jvmTopology = JVMTopology(self)

  override def aroundReceive(receive: Receive, msg: Any): Unit = {
    logger.debug(s"Around Receive: $msg")
    msg match {
      case message: TheLeaderIs => // We only receive a "TheLeaderIs" if the state changed
        logger.warn(s"Got a TheLeaderIs message from the manager: $message")
        manager = message.leader
        // We need to send all worker type info (even if the manager has just switched, we don't care)
        manager match {
          case Some(currentManager) =>
            logger.debug("Send all workerTypeInfo to the manager")
            allWorkerTypeInfo.foreach(workerTypeInfo => {
              currentManager ! workerTypeInfo
            })
          case None => // Nothing to do
        }
      case clusterMessage: ClusterRemoteMessage =>
        // Message used for the administration, we execute it
        handleClusterRemoteMessage(clusterMessage)
      case terminatingActor: Terminated =>
        // We watch our own actors, but also the Manager
        if(jvmTopology.removeWorkerActor(terminatingActor.actor)) {
          logger.warn(s"Got a Terminated message from a WorkerActor (${terminatingActor.actor}), it has been removed from our jvm topology.")
        } else if(manager.isDefined && manager.get == terminatingActor.actor) {
          logger.warn("Got a Terminated message from the Manager, we let the actors continue their job and wait for a new Manager to come in.")
        } else {
          logger.error(s"Got a Terminated message from a unknown actor: ${terminatingActor.actor}")
        }
      case x =>
        // Message used by the developer using the library, we forward it
        logger.warn("You should not use the WorkerLeader for your own messages. We accept it for now but you should avoid that.")
        receive(msg)
    }
  }

  /**
    * Handle all cluster messages.
    */
  final private def handleClusterRemoteMessage(clusterRemoteMessage: ClusterRemoteMessage) = Try {
    // The entire method is surrounded by a Try to be sure we don't crash for any reason. But we should handle every
    // message correctly by default
    clusterRemoteMessage match {
      case message: ClusterTopology =>
        Communication.setClusterTopology(message)
      case message: StartWorkerActor =>
        Try{startWorker(message.workerTypeId)} match {
          case Success(res) =>
            // Setting a watcher can leads to failure if the actors just dies at that moment
            Try{context.watch(res)} match {
              case Success(s) =>
                val workerActorHealth = WorkerActorHealth(self, message.workerTypeId, res)
                jvmTopology.addWorkerActor(workerActorHealth)
                message.actorRef ! StartedWorkerActor(self, message, res)
              case Failure(e) =>
                logger.error(s"Impossible to set a watcher, the actor probably died mid-way: $e")
                message.actorRef ! FailedWorkerActor(self, message, new Exception("Failing actor after starting it"))
            }
          case Failure(err) =>
            val excep = err match {
              case exception: Exception => exception
              case _ => new Exception(s"Unknown error while starting a workerActor: $err")
            }
            message.actorRef ! FailedWorkerActor(self, message, excep)
        }
      case x =>
        logger.error(s"Unknown ClusterRemoteMessage received $x, this should never happen!")
    }
  }

  /**
    * Class to implement by the developer.
    * @param workerTypeId
    */
  def startWorker(workerTypeId: String): ActorRef

  /**
    * List of available WorkerActors given by the developer in the current jvm.
    */
  def allWorkerTypeInfo: List[WorkerTypeInfo]
}
