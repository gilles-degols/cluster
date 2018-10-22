package net.degols.filesgate.libs.cluster.manager

import javax.inject.{Inject, Singleton}

import akka.actor.ActorRef
import net.degols.filesgate.libs.cluster.messages.IAmTheWorkerLeader
import net.degols.filesgate.libs.election._
import org.slf4j.LoggerFactory

/**
  * Handle the cluster in itself. We should only have a limited number of available Watchers as the election process
  * is planned for 3 nodes (maybe 5), not much more. The other nodes can act as followers to the Manager without any
  * overhead on the election system.
  * There is always one Manager instance per jvm, but if the jvm is not in the election configuration, it won't be
  * considered as taking part in the election, it will simply act as a watcher.
  * @param electionService
  * @param configurationService
  */
@Singleton
class Manager @Inject()(electionService: ElectionService, configurationService: ConfigurationService) extends ElectionWrapper(electionService, configurationService){
  private val logger = LoggerFactory.getLogger(getClass)
  private var _previousLeader: Option[ActorRef] = None
  private var _currentWorkerLeader: Option[ActorRef] = None

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
      }
    case message =>
      logger.debug(s"[Manager] Received unknown message: $message")
  }
}
