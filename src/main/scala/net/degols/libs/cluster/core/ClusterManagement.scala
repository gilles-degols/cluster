package net.degols.libs.cluster.core

import akka.actor.{ActorContext, ActorRef}
import net.degols.libs.cluster.balancing.LoadBalancer
import net.degols.libs.cluster.messages._
import net.degols.libs.election.Tools
import net.degols.libs.cluster.{ClusterTools => ClusterTools}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
import javax.inject.{Inject, Singleton}

/**
  * Basic API to the Cluster class for internal use only. This avoid exposing too much the Cluster class.
  * Typically, it handles everything from the reception of WorkerTypeInfo to start/stop of actors. But it
  * does not handle the load balancing in itself
  */
@Singleton
class ClusterManagement(context: ActorContext, val cluster: Cluster) {
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

  def registerWorkerTypeOrder(workerTypeOrder: WorkerTypeOrder): Unit = {
    cluster.registerWorkerTypeOrder(workerTypeOrder)
    cluster.watchWorkerTypeOrder(context, workerTypeOrder)
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
    * From time to time we can receive an update on the health of a given WorkerActor.
    */
  def updateWorkerActorHealth(workerActorHealth: WorkerActorHealth): Unit = {
    cluster.updateWorkerActorHealth(_clusterTopology, workerActorHealth)
  }

  def registerFailedWorkerActor(failedWorkerActor: FailedWorkerActor): Unit = {
    cluster.registerFailedWorkerActor(_clusterTopology, failedWorkerActor)
  }

  def cleanOldWorkers(): Unit = {
    cluster.cleanOldWorkers()
  }

  def distributeWorkers(loadBalancers: Seq[LoadBalancer], softDistribution: Boolean): Unit = {
    // Lookup to easily find the orders for a given workerType
    val orders = cluster.ordersByWorkerTypeId()

    // For each WorkerType we need to find the appropriate load balancer, then ask him to do the work distribution
    cluster.nodesByWorkerType().keys
      .flatMap(workerType => {
        // We execute the load balancer for every order we have for the given workerType
        val ordersForType = orders.getOrElse(workerType.workerTypeInfo.workerTypeId, List.empty[WorkerTypeOrder])
          .map(order => (workerType, order))

        if(ordersForType.size >= 2) {
          logger.info(s"We have ${ordersForType.size} different orders for ${workerType.workerTypeInfo.workerTypeId}")
        } else if (ordersForType.isEmpty)(
          // We cannot simply stop all related actors, as we also need to handle the lost of specific orders, in that
          // case we need to specifically target actors related to the lost orders. Because of that, we directly stop
          // the actors of a related workOrder as soon as we received the Terminated message
          logger.warn(s"There is no remaining orders for ${workerType.workerTypeInfo.workerTypeId}, normally no related" +
            s" actors should exist anymore (to verify).")
        )

        ordersForType
      })
      .foreach(raw => {
        val workerType = raw._1
        val order = raw._2

        // Find the appropriate load balancer for the current order & type
        loadBalancers.find(_.isLoadBalancerType(order.loadBalancerType)) match {
          case Some(loadBal) =>
            Try{
              if(softDistribution) {
                loadBal.softWorkDistribution(workerType, order)
              } else {
                loadBal.hardWorkDistribution(workerType, order)
              }
            } match {
              case Success(res) => // Nothing to do
              case Failure(err) => logger.error(s"Exception occurred while trying to distribute the work of $workerType: ${ClusterTools.formatStacktrace(err)}")
            }
          case None =>
            logger.error(s"There is no loadBalancer accepting the type ${order.loadBalancerType}!")
        }
    })
  }

  /**
    * Send ClusterTopology to every WorkerLeader
    */
  def sendClusterTopology(): Unit = {
    cluster.nodes.flatMap(_.workerManagers.map(_.actorRef)).foreach(workerManagerRef => workerManagerRef ! _clusterTopology)
  }

  /**
    * Every time we receive a Terminated() message from a watched actor, we need to remove it. We do not trigger a new
    * balancing directly, as most of the time all the actors in one JVM will fail at the same time. That would trigger
    * a lot of identical requests to re-distribute the workers. It is better to simply wait for the SoftWorkerDistribution
    * message automatically sends every few seconds.
    *
    * If the actorRef is linked to the initiator of a WorkerTypeOrder, remove it. If other sent the same order, no problem.
    * If no-one is alive anymore, the Cluster will directly remove the related actors
    */
  def removeWatchedActor(actorRef: ActorRef): Unit = {
    cluster.registerFailedWorkerOrderSender(context, actorRef)

    if(!cluster.registerFailedWorkerActor(actorRef)) {
      cluster.registerFailedWorkerLeader(actorRef)
    }
  }
}

