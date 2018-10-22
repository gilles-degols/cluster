package net.degols.filesgate.libs.cluster.balancing

import akka.actor.ActorContext
import net.degols.filesgate.libs.cluster.core.Cluster
import net.degols.filesgate.libs.cluster.messages.ClusterTopology
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
}
