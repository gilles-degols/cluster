package net.degols.filesgate.libs.cluster.balancing

import akka.actor.{ActorContext, ActorRef}
import net.degols.filesgate.libs.cluster.core.Cluster
import net.degols.filesgate.libs.cluster.messages.{ClusterTopology, StartedWorkerActor, WorkerTypeInfo}
import org.slf4j.LoggerFactory

/**
  * In charge of the load balancing of the Manager. Typically, it handles everything from the reception of WorkerTypeInfo
  * to start/stop of actors. Only a limited subset of the methods must be implemented to provide a custom load balancer.
  */
abstract class LoadBalancing(context: ActorContext, cluster: Cluster) {
  private val logger = LoggerFactory.getLogger(getClass)

  // If the manager is a follower, it will simply overrides this ClusterTopology based on what it receives from a leader
  // That way we can easily handle a switch of Manager without downtime. At that moment it will need to re-construct the internal Cluster class
  private var _clusterTopology: ClusterTopology = ClusterTopology(context.self)

  def setClusterTopology(clusterTopology: ClusterTopology): Unit = {
    logger.warn("ClusterTopology is being set. Re-construct the hierarchy of JVMs to take over nicely.")
    _clusterTopology = clusterTopology
    cluster.reconstructFromClusterTopology(clusterTopology)
  }

  /**
    * Every WorkerLeader sends its WorkerInfo from time to time. We have to store it (to know which JVM can start actors
    * of a specific type and so on)
    */
  def registerWorkerTypeInfo(workerTypeInfo: WorkerTypeInfo): Unit = {
    cluster.registerWorkerTypeInfo(workerTypeInfo)
    cluster.watchWorkerTypeInfo(context, workerTypeInfo)
  }

  /**
    * When a WorkerActor is started, we want to save its status, and watch its actor.
    * @param startedWorkerActor
    */
  def registerStartedWorkerActor(startedWorkerActor: StartedWorkerActor): Unit = {
    cluster.registerStartedWorkerActor(startedWorkerActor)
    cluster.watchWorkerActor(context, startedWorkerActor)
  }

  /**
    * Check every Node, WorkerType, etc. to be sure that we have the appropriate number of started actors.
    * This method is called from time to time.
    */
  def distributeWorkers(): Unit = ???

  /**
    * Every time we receive a Terminated() message from a watched actor, we need to remove it. We do not trigger a new
    * balancing directly, as most of the time all the actors in one JVM will fail at the same time. That would trigger
    * a lot of identical requests to re-distribute the workers. It is better to simply wait for the SoftWorkerDistribution
    * message automatically sends every few seconds.
    */
  def removeWatchedActor(actorRef: ActorRef): Unit = {
    if(!cluster.registerFailedWorkerActor(actorRef)) {
      cluster.registerFailedWorkerLeader(actorRef)
    }
  }
}
