package net.degols.filesgate.libs.cluster.manager

import javax.inject.{Inject, Singleton}

import akka.actor.{ActorRef, Cancellable}
import net.degols.filesgate.libs.cluster.ClusterConfiguration
import net.degols.filesgate.libs.cluster.balancing.LoadBalancing
import net.degols.filesgate.libs.cluster.core.{Cluster, ClusterManagement}
import net.degols.filesgate.libs.cluster.messages.{DistributeWork, IAmTheWorkerLeader}
import net.degols.filesgate.libs.election._
import org.slf4j.LoggerFactory

/**
  * Handle the cluster in itself. We should only have a limited number of available Watchers as the election process
  * is planned for 3 nodes (maybe 5), not much more. The other nodes can act as followers to the Manager without any
  * overhead on the election system.
  * There is always one Manager instance per jvm, but if the jvm is not in the election configuration, it won't be
  * considered as taking part in the election, it will simply act as a watcher.
  * @param electionService
  * @param clusterConfiguration
  */
@Singleton
final class Manager @Inject()(electionService: ElectionService, clusterConfiguration: ClusterConfiguration, cluster: Cluster, loadBalancing: LoadBalancing) extends ElectionWrapper(electionService, configurationService){
  private val logger = LoggerFactory.getLogger(getClass)
  private var _previousLeader: Option[ActorRef] = None
  private var _currentWorkerLeader: Option[ActorRef] = None

  /**
    * Specific objects to cancel the scheduling of messages sent to distribute the work
    */
  private var _scheduleSoftWork: Option[Cancellable] = None
  private var _scheduleHardWork: Option[Cancellable] = None

  /**
    * The clusterManagement and LoadBalancing should nothing by themselves to distribute work or so on. They should
    * rely on automatic call to a limited number of methods once the
    */
  protected var clusterManagement = new ClusterManagement(context, cluster, loadBalancing)

  // To easily instantiate the LoadBalancing, some attributes are given manually here
  loadBalancing.clusterManagement = clusterManagement
  loadBalancing.context = context

  override def receive: Receive = {
    case IAmTheWorkerLeader =>
      // When the WorkerLeader starts, it contacts the WorkerLeader. Afterwards, it will be used by the Manager to send
      // information about the leader status
      _currentWorkerLeader = Option(sender())
      _currentWorkerLeader.get ! TheLeaderIs(_previousLeader)

    case IAmLeader | IAmFollower | TheLeaderIs =>
      // The current leader might have changed, we are not sure yet
      if(_previousLeader != currentLeader) {
        logger.warn(s"The Manager in charge has just changed from ${_previousLeader} to $currentLeader")
        _previousLeader = currentLeader
        _currentWorkerLeader match {
          case Some(workerLeader) => workerLeader ! currentLeader
          case None => logger.warn("The WorkerLeader is not yet started, it will be warned once it contacts the Manager")
        }

        if(isLeader) {
          scheduleWorkDistribution()
        } else {
          unscheduleWorkDistribution()
        }
      }

    case message: DistributeWork =>
      if(isLeader) { // There is no reason to distribute work if we are not leader
        logger.debug(s"Distribute workers (soft: $message)")
        if(message.soft) {
          loadBalancing.softWorkDistribution()
        } else {
          loadBalancing.hardWorkDistribution()
        }
      }

    case message =>
      logger.debug(s"[Manager] Received unknown message: $message")
  }

  private def scheduleWorkDistribution(): Unit = {
    _scheduleSoftWork = Option(context.system.scheduler.schedule(clusterConfiguration.softWorkDistributionFrequency, clusterConfiguration.softWorkDistributionFrequency, self, DistributeWork(true)))
    _scheduleHardWork = Option(context.system.scheduler.schedule(clusterConfiguration.hardWorkDistributionFrequency, clusterConfiguration.hardWorkDistributionFrequency, self, DistributeWork(false)))
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
  }
}
