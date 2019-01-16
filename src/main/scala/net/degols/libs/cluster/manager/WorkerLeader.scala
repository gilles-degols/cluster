package net.degols.libs.cluster.manager

import akka.actor.{Actor, ActorRef, Cancellable, Kill, Props, Terminated}
import com.google.inject.{Inject, Singleton}
import net.degols.libs.cluster.balancing.LoadBalancer
import net.degols.libs.cluster.core.Cluster
import net.degols.libs.cluster.{ClusterConfiguration, Tools}
import net.degols.libs.cluster.messages._
import net.degols.libs.election._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Internal message to verify if we might be re-connected to a manager after a short network problem
  */
case object IsStillDisconnectedFromManager

/**
  * Manage the various Workers in the current JVM, only one instance is allowed, hence the Singleton. This is the actor
  * knowing which WorkerActors are available, and how to start them.
  */
@Singleton
abstract class WorkerLeader @Inject()(electionService: ElectionService, configurationService: ConfigurationService, clusterConfiguration: ClusterConfiguration, cluster: Cluster)(implicit val ec: ExecutionContext) extends Actor{
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Scheduled message to verify if we are still disconnected from the manager in charge after some time. If yes, we
    * need to kill every actor of our current jvm
    */
  private var checkManagerDisconnection: Option[Cancellable] = None


  /**
    * Custom User LoadBalancer, they do not need to exist, it's just for advanced users. A reference to their instances
    * will be sent to local manager only (so they do not need to be serializable)
    */
  protected val userLoadBalancers: List[LoadBalancer] = List.empty[LoadBalancer]

  /**
    * Start the local Manager in charge of the election. It's not necessarily the manager in charge
    */
  val localManager: ActorRef = context.actorOf(Props.create(classOf[Manager], electionService, configurationService, clusterConfiguration, cluster), name = "LocalManager")
  override def preStart(): Unit = {
    super.preStart()
    // Inform the localManager of our existence and give the optional UserLoadBalancer objects
    localManager ! IAmTheWorkerLeader(userLoadBalancers)
  }

  /**
    * To easily handle multiple JVMs with a lot of different actors, we might want to structure a bit their name
    */
  val COMPONENT: String = "Component"
  val PACKAGE: String = "Package"

  /**
    * Number of started Actors locally, to be sure to not have any conflict in their name
    */
  var startedWorkers: Long = 0L

  /**
    * In very specific case the developer might wants to override this value
    */
  val nodeInfo: NodeInfo = {
    val networkHostname = Tools.remoteActorPath(self).split("@")(1).split(":").head
    val localHostname = clusterConfiguration.localHostname
    NodeInfo(networkHostname, localHostname)
  }

  /**
    * Current Manager in charge of the cluster (it can be different than the localManager obviously)
    */
  var manager: Option[ActorRef] = None

  /**
    * Information about the current topology we have in this jvm
    */
  lazy val jvmTopology = JVMTopology(self)

  // TODO: Add suicide when we didn't get a new Manager in a short amount of time. Or better: send a message to all
  // actors to stop their work, and it will be resumed by the manager
  override def aroundReceive(receive: Receive, msg: Any): Unit = {
    logger.debug(s"[WorkerLeader] Around Receive: $msg")
    msg match {
      case message: TheLeaderIs => // We only receive a "TheLeaderIs" if the state changed
        logger.warn(s"Got a TheLeaderIs message from the manager: $message")
        manager = message.leaderWrapper // message.leader is only used for the election != cluster management as the cluster management owns the election actor.
        // We need to send all worker type info (even if the manager has just switched, we don't care)
        manager match {
          case Some(currentManager) =>
            logger.debug(s"Send all workerTypeInfo to the manager ($currentManager)")
            allWorkerTypeInfo.foreach(workerTypeInfo => {
              val completeWorkerTypeId: String = Communication.fullActorName(COMPONENT, PACKAGE, workerTypeInfo.workerTypeId)
              val prettyWorkerTypeInfo = WorkerTypeInfo(workerTypeInfo.actorRef, completeWorkerTypeId, workerTypeInfo.loadBalancerType, workerTypeInfo.metadata)
              prettyWorkerTypeInfo.nodeInfo = nodeInfo
              currentManager ! prettyWorkerTypeInfo
            })
          case None => // Nothing to do
        }
      case clusterMessage: ClusterRemoteMessage =>
        // Message used for the administration, we execute it
        handleClusterRemoteMessage(clusterMessage)

      case IsStillDisconnectedFromManager =>
        manager match {
          case Some(m) =>
            logger.warn("We just re-check if we are still disconnected from the Manager and it seems we got it back. We let workers continue their job. Be sure to have implemented a hard-load-balancing in charge of killing workers in excess.")
          case None =>
            logger.warn("We just re-check if we are still disconnected from the Manager and it seems we still don't have it, so we kill all our workers.")
            jvmTopology.workerActors.values.flatMap(_.map(_.actorRef)).foreach(_ ! Kill)
        }

      case terminatingActor: Terminated =>
        // We watch our own actors, but also the Manager
        if(jvmTopology.removeWorkerActor(terminatingActor.actor)) {
          logger.warn(s"Got a Terminated message from a WorkerActor (${terminatingActor.actor}), it has been removed from our jvm topology.")
        } else if(manager.isDefined && manager.get == terminatingActor.actor) {
          checkManagerDisconnection.map(c => c.cancel())
          logger.warn(s"Got a Terminated message from the Manager, we let the actors continue their job during ${clusterConfiguration.watcherTimeoutBeforeSuicide.toSeconds} seconds and wait for a new Manager to come in.")
          checkManagerDisconnection = Option(context.system.scheduler.scheduleOnce(clusterConfiguration.watcherTimeoutBeforeSuicide, self, IsStillDisconnectedFromManager))
        } else {
          logger.error(s"Got a Terminated message from a unknown actor: ${terminatingActor.actor}")
        }
      case x =>
        // Message used by the developer using the library, we forward it
        logger.warn(s"You should not use the WorkerLeader for your own messages (message: $x). We accept it for now but you should avoid that.")
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
        logger.info(s"[WorkerLeader] Received ClusterTopology: $message")
        Communication.setClusterTopology(message)
      case message: StartWorkerActor =>
        // The worker name is not mandatory, it's just to avoid having the developer deals with it if it does not need to
        logger.info(s"Starting worker type id: ${message.workerTypeInfo.workerTypeId}")
        val workerName = s"${message.workerTypeInfo.workerTypeId}-$startedWorkers"
        startedWorkers += 1
        val initialName = message.workerTypeInfo.workerTypeId.split(":").drop(2).mkString(":")

        Try{startWorker(initialName, workerName)} match {
          case Success(res) =>
            // Setting a watcher can lead to failure if the actors just die at that moment
            Try{context.watch(res)} match {
              case Success(s) =>
                val workerActorHealth = WorkerActorHealth(self, message.workerTypeInfo, res, nodeInfo, self, message.workerId)
                jvmTopology.addWorkerActor(workerActorHealth)
                val m = StartedWorkerActor(self, message, res)
                m.nodeInfo = nodeInfo
                message.actorRef ! m
                message.actorRef ! workerActorHealth
              case Failure(e) =>
                logger.error(s"Impossible to set a watcher, the actor probably died mid-way: $e")
                val m = FailedWorkerActor(self, message, new Exception("Failing actor after starting it"))
                m.nodeInfo = nodeInfo
                message.actorRef ! m
            }
          case Failure(err) =>
            val excep = err match {
              case exception: Exception => exception
              case _ => new Exception(s"Unknown error while starting a workerActor: $err")
            }
            logger.error(s"Got an exception while trying to start a worker ${initialName}: ${Tools.formatStacktrace(excep)}")
            val m = FailedWorkerActor(self, message, excep)
            m.nodeInfo = nodeInfo
            message.actorRef ! m
        }
      case x =>
        logger.error(s"Unknown ClusterRemoteMessage received $x, this should never happen!")
    }
  }

  /**
    * We start the related actor
    * @param workerTypeId
    */
  def startWorker(workerTypeId: String, actorName: String): ActorRef

  /**
    * List of available WorkerActors given by the developer in the current jvm.
    */
  def allWorkerTypeInfo: List[WorkerTypeInfo]
}
