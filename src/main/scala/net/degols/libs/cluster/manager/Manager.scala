package net.degols.libs.cluster.manager

import javax.inject.{Inject, Singleton}

import akka.actor.{ActorRef, Cancellable, Terminated}
import net.degols.libs.cluster.ClusterConfiguration
import net.degols.libs.cluster.balancing.{BasicLoadBalancer, LoadBalancer}
import net.degols.libs.cluster.core.{Cluster, ClusterManagement}
import net.degols.libs.cluster.messages._
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
final class Manager @Inject()(electionService: ElectionService,
                              configurationService: ConfigurationService,
                              clusterConfiguration: ClusterConfiguration,
                              cluster: Cluster)
                    extends ElectionWrapper(electionService, configurationService){
  private val logger = LoggerFactory.getLogger(getClass)
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
  protected val clusterManagement = new ClusterManagement(context, cluster)

  /**
    * Custom User LoadBalancer, they do not need to exist, it's just for advanced users. They can be set by the workerLeader once
    * it's initialized.
    */
  protected var userLoadBalancers: List[LoadBalancer] = List.empty[LoadBalancer]

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
        logger.debug(s"[Manager] Distribute workers (soft: $message)")
        clusterManagement.distributeWorkers(loadBalancers, message.soft)
      }

    case message: CleanOldWorkers =>
      if(isLeader){
        logger.debug("[Manager] Clean old workers")
        clusterManagement.cleanOldWorkers()
      }

    case message: ClusterRemoteMessage =>
      logger.debug(s"[Manager] Received a ClusterRemoteMessage: $message")
      if(isLeader) {
        handleClusterMessage(message)
      }

    case message: Terminated =>
      logger.warn(s"[Manager] Received a Terminated message from ${message.actor}")
      clusterManagement.removeWatchedActor(message.actor)

    case message =>
      logger.debug(s"[Manager] Received unknown message: $message")
  }

  def checkChangedLeader(): Unit = {
    if(_previousLeader != currentLeader) {
      logger.warn(s"[Manager] The Manager in charge has just changed from ${_previousLeader} to $currentLeader, the wrapper changed from ${_previousLeaderWrapper} to $currentLeaderWrapper")
      _previousLeader = currentLeader
      _previousLeaderWrapper = currentLeaderWrapper
      _currentWorkerLeader match {
        case Some(workerLeader) => workerLeader ! TheLeaderIs(currentLeader, currentLeaderWrapper)
        case None => logger.warn("[Manager] The WorkerLeader is not yet started, it will be warned once it contacts the Manager")
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
        clusterManagement.registerWorkerTypeInfo(message)
      case message: WorkerTypeOrder =>
        clusterManagement.registerWorkerTypeOrder(message)
      case message: WorkerActorHealth =>
        clusterManagement.updateWorkerActorHealth(message)
      case message: StartedWorkerActor =>
        clusterManagement.registerStartedWorkerActor(message)
        clusterManagement.sendClusterTopology()
      case message: FailedWorkerActor =>
        clusterManagement.registerFailedWorkerActor(message)
        clusterManagement.sendClusterTopology()
      case other =>
        logger.error(s"Received unknown ClusterRemoteMessage: $rawMessage")
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
