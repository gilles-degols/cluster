package net.degols.libs.cluster.balancing

import akka.actor.{ActorContext, ActorRef}
import com.google.inject.Inject
import net.degols.libs.cluster.core.{Cluster, ClusterManagement, WorkerType}
import net.degols.libs.cluster.messages.{ClusterTopology, LoadBalancerType, StartedWorkerActor, WorkerTypeInfo}
import org.slf4j.LoggerFactory

/**
  * In charge of the load balancing of the Manager.  Only a limited subset of the methods must be implemented to provide a custom load balancer.
  * To ease the extension of this class, some attributes are not automatically asked at creation. They are only set up
  * by the Manager.
  */
abstract class LoadBalancer {
  private val logger = LoggerFactory.getLogger(getClass)

  // Set by the Manager
  var context: ActorContext = _

  // Set by the Manager
  var clusterManagement: ClusterManagement = _

  /**
    * Return the related class LoadBalancerType this class is related to. Only a one-to-one relation
    */
  def isLoadBalancerType(loadBalancerType: LoadBalancerType): Boolean

  /**
    * Check a given WorkerType, etc. to be sure that we have the appropriate number of started actors for each WorkerManager
    * This method is called from time to time. The "soft" mode is called every few seconds to create missing actors, the "hard" mode
    * is called once every few minutes to optionnally stop actors in some JVMs and start them elsewhere.
    */
  def softWorkDistribution(workerType: WorkerType): Unit

  def hardWorkDistribution(workerType: WorkerType): Unit
}
