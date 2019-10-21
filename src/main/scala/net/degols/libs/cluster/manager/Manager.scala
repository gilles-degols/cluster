package net.degols.libs.cluster.manager

import javax.inject.{Inject, Singleton}
import akka.actor.{ActorRef, ActorSystem, Cancellable, Terminated}
import net.degols.libs.cluster.balancing.{BasicLoadBalancer, LoadBalancer}
import net.degols.libs.cluster.configuration.{ClusterConfiguration, ClusterConfigurationApi, DefaultClusterConfiguration}
import net.degols.libs.cluster.core.{Cluster, ClusterManagement}
import net.degols.libs.cluster.messages._
import net.degols.libs.cluster.utils.Logging
import net.degols.libs.election._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Handle the cluster in itself. We should only have a limited number of available Watchers as the election process
  * is planned for 3 nodes (maybe 5), not much more. The other nodes can act as followers to the Manager without any
  * overhead on the election system.
  * There is always one Manager instance per jvm, but if the jvm is not in the election configuration, it won't be
  * considered as taking part in the election, it will simply act as a watcher.
  * We can provide multiple types of LoadBalancer
  * @param electionService
  * @param clusterConfiguration
  */
@Singleton
final class Manager(electionService: ElectionService,
                    electionConfiguration: ElectionConfigurationApi,
                    clusterConfiguration: ClusterConfiguration,
                    cluster: Cluster,
                    actorSystem: ActorSystem)
                    extends ElectionWrapper(electionService, electionConfiguration, actorSystem) with Logging{

  private var _previousLeader: Option[ActorRef] = None
  private var _previousLeaderWrapper: Option[ActorRef] = None
  private var _currentWorkerLeader: Option[ActorRef] = None

  /**
    * Specific objects to cancel the scheduling of messages sent to distribute the work
    */
  private var _scheduleSoftWork: Option[Cancellable] = None
  private var _scheduleHardWork: Option[Cancellable] = None
  private var _scheduleCleaning: Option[Cancellable] = None

  /**
    * The clusterManagement and LoadBalancer should do nothing by themselves to distribute work or so on. They should
    * rely on automatic calls to a limited number of methods
    */
  protected val clusterManagement = new ClusterManagement(context, cluster, clusterConfiguration)

  /**
    * Custom User LoadBalancer, they do not need to exist, it's just for advanced users. They can be set by the workerLeader once
    * it's initialized.
    */
  protected var userLoadBalancers: Seq[LoadBalancer] = List.empty[LoadBalancer]

  /**
    * All available load balancers, the one proposed by default + the user one.
    */
  private lazy val loadBalancers = {
    val loadBalancers = List(new BasicLoadBalancer()) ++ userLoadBalancers

    // To easily instantiate the LoadBalancer, some attributes are given manually here
    loadBalancers.foreach(loadBalancer => {
      loadBalancer.clusterManagement = clusterManagement
      loadBalancer.context = context
    })

    loadBalancers
  }


  override def receive: Receive = {
    case x: IAmTheWorkerLeader =>
      // When the WorkerLeader starts, it contacts the WorkerLeader. Afterwards, it will be used by the Manager to send
      // information about the leader status
      userLoadBalancers = x.userLoadBalancers
      _currentWorkerLeader = Option(sender())
      _currentWorkerLeader.get ! TheLeaderIs(_previousLeader, _previousLeaderWrapper)
    case x: TheLeaderIs =>
      // The current leader might have changed, we are not sure yet
      checkChangedLeader()

    case IAmLeader | IAmFollower =>
      checkChangedLeader()

    case message: DistributeWork =>
      if(isLeader) { // There is no reason to distribute work if we are not leader
        debug(s"[Manager] Distribute workers (soft: $message)")
        clusterManagement.distributeWorkers(loadBalancers, message.soft)
      }

    case message: CleanOldWorkers =>
      if(isLeader){
        debug("[Manager] Clean old workers")
        clusterManagement.cleanOldWorkers()
      }

    case message: ClusterRemoteMessage =>
      debug(s"[Manager] Received a ClusterRemoteMessage: $message")
      if(isLeader) {
        handleClusterMessage(message)
      }

    case message: Terminated =>
      warn(s"[Manager] Received a Terminated message from ${message.actor}")
      clusterManagement.removeWatchedActor(message.actor)

    case message =>
      warn(s"[Manager] Received unknown message: $message")
  }

  def checkChangedLeader(): Unit = {
    if(_previousLeader != currentLeader) {
      warn(s"[Manager] The Manager in charge has just changed from ${_previousLeader} to $currentLeader, the wrapper changed from ${_previousLeaderWrapper} to $currentLeaderWrapper")
      _previousLeader = currentLeader
      _previousLeaderWrapper = currentLeaderWrapper
      _currentWorkerLeader match {
        case Some(workerLeader) => workerLeader ! TheLeaderIs(currentLeader, currentLeaderWrapper)
        case None => warn("[Manager] The WorkerLeader is not yet started, it will be warned once it contacts the Manager")
      }

      if(isLeader) {
        scheduleWorkDistribution()
      } else {
        unscheduleWorkDistribution()
      }
    }
  }

  def handleClusterMessage(rawMessage: ClusterRemoteMessage): Unit = {
    rawMessage match {
      case message: WorkerTypeInfo =>
        debug(s"Register WorkerTypeInfo: $message, metadata is ${message.metadata}")
        clusterManagement.registerWorkerTypeInfo(message)
        sender() ! MessageWasHandled(context.self,message)
      case message: WorkerTypeOrder =>
        debug(s"Register WorkerTypeOrder: $message, metadata is ${message.metadata}")
        clusterManagement.registerWorkerTypeOrder(message)
        sender() ! MessageWasHandled(context.self,message)
      case message: WorkerActorHealth =>
        debug(s"Register WorkerActorHealth: $message")
        clusterManagement.updateWorkerActorHealth(message)
        sender() ! MessageWasHandled(context.self,message)
      case message: StartedWorkerActor =>
        debug(s"Register StartedWorkerActor: $message, metadata is ${message.startWorkerActor.workerTypeOrder.metadata}")
        clusterManagement.registerStartedWorkerActor(message)
        sender() ! MessageWasHandled(context.self,message)
      case message: FailedWorkerActor =>
        debug(s"Register FailedWorkerActor: $message")
        clusterManagement.registerFailedWorkerActor(message)
        sender() ! MessageWasHandled(context.self,message)
      case message: GetInfoFromActorRef =>
        debug(s"Worker ${sender()} is asking some information about the actorRef ${message.targetActorRef}")
        sender() ! clusterManagement.infoFromActorRef(message.targetActorRef)
      case message: GetActorRefsFor =>
        debug(s"Worker ${sender()} is asking for the ActorRefs of workerTypeId: ${message.workerTypeId} and orderId: ${message.orderId}")
        Try {
          val actorRefs = clusterManagement.actorRefsFor(message.workerTypeId, message.orderId, message.isRunning)
          sender() ! actorRefs
        } match {
          case Success(res) => // Nothing to do
          case Failure(err) => error(s"Impossible to reply to ${sender()} about the actorRefs for the WorkerTypeId: ${message.workerTypeId} and orderId: ${message.orderId}")
        }
      case message: GetAllWorkerTypeIds =>
        debug(s"Worker ${sender()} is asking for the existing workerTypeIds in the system.")
        Try {
          val workerTypeIds = clusterManagement.existingWorkerTypeIds()
          sender() ! workerTypeIds
        } match {
          case Success(res) => // Nothing to do
          case Failure(err) => error(s"Impossible to reply to ${sender()} about the existing WorkerTypeIds")
        }

      case other =>
        error(s"Received unknown ClusterRemoteMessage: $rawMessage")
    }
  }

  private def scheduleWorkDistribution(): Unit = {
    _scheduleSoftWork = Option(context.system.scheduler.schedule(clusterConfiguration.softWorkDistributionFrequency, clusterConfiguration.softWorkDistributionFrequency, self, DistributeWork(true)))
    _scheduleHardWork = Option(context.system.scheduler.schedule(clusterConfiguration.hardWorkDistributionFrequency, clusterConfiguration.hardWorkDistributionFrequency, self, DistributeWork(false)))
    _scheduleCleaning = Option(context.system.scheduler.schedule(clusterConfiguration.startWorkerTimeout, clusterConfiguration.startWorkerTimeout, self, CleanOldWorkers()))
  }

  private def unscheduleWorkDistribution(): Unit = {
    _scheduleSoftWork match {
      case Some(s) => s.cancel()
      case None =>
    }
    _scheduleHardWork match {
      case Some(s) => s.cancel()
      case None =>
    }
    _scheduleCleaning match {
      case Some(s) => s.cancel()
      case None =>
    }
  }
}
