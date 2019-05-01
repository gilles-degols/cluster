package net.degols.libs.cluster.core

import javax.inject.Singleton
import akka.actor.{ActorContext, ActorRef, ActorSystem}
import com.google.inject.Inject
import net.degols.libs.cluster.ClusterConfiguration
import net.degols.libs.cluster.messages._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Random, Success, Try}
import scala.collection.mutable

/**
  *
  * @param order
  * @param actors Set of actors initiating the orders. Those are NOT the workers themselves
  */
case class WorkerTypeOrderWrapper(order: WorkerTypeOrder, actors: mutable.Set[ActorRef])

/**
  * General interface to access information about the cluster
  */
@Singleton
class Cluster @Inject()(clusterConfiguration: ClusterConfiguration) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * All WorkerTypeOrder received from any JVM of the cluster. The duplicate orders are automatically removed before
    * being stored here.
    * Orders will be automatically removed if all the related JVM initiating the orders are dead
    * TODO: Avoid going through 1000s elements for each soft-work distribution
    */
  private var _orders: mutable.Map[String, WorkerTypeOrderWrapper] = mutable.Map[String, WorkerTypeOrderWrapper]()

  /**
    * All instances of Nodes found on the cluster
    */
  private var _nodes: Seq[Node] = List.empty[Node]

  /**
    * When we receive a WorkerTypeInfo, we store the information (if not already done), which include the related
    * node found (might be new), the InstanceTypeId, ...
    */
  def registerWorkerTypeInfo(workerTypeInfo: WorkerTypeInfo): Unit = {
    val rawNode = Node.fromNodeInfo(workerTypeInfo.nodeInfo)
    val rawWorkerManager = WorkerManager.fromWorkerTypeInfo(workerTypeInfo)
    val rawWorkerType = WorkerType.fromWorkerTypeInfo(workerTypeInfo)
    val node = addNode(rawNode)
    val workerManager = node.addWorkerManager(rawWorkerManager)
    workerManager.addWorkerType(rawWorkerType)
  }

  def registerWorkerTypeOrder(workerTypeOrder: WorkerTypeOrder): Unit = {
    _orders.get(workerTypeOrder.id) match {
      case Some(wrapper) =>
        wrapper.actors.add(workerTypeOrder.actorRef)
      case None=>
        val actors = mutable.Set(workerTypeOrder.actorRef)
        _orders.put(workerTypeOrder.id, WorkerTypeOrderWrapper(workerTypeOrder, actors))
    }
  }

  /**
    * Return a mapping of all WorkerTypeId -> List(WorkerTypeOrder). We can have multiple WorkerTypeOrder for a given
    * WorkerTypeId as different orders could come from multiple JVMs
    * @return
    */
  def ordersByWorkerTypeId(): Map[String, Seq[WorkerTypeOrder]] = {
    _orders.groupBy(_._2.order.workerTypeId).map(raw => raw._1 -> raw._2.values.map(_.order).toList)
  }

  /**
    * Every time we add a WorkerTypeInfo we probably want to watch the status of the various actors, to be notified
    * when they die
    * @param workerTypeInfo
    */
  def watchWorkerTypeInfo(context: ActorContext, workerTypeInfo: WorkerTypeInfo): Try[ActorRef] = {
    // A watch can fail if the actor is failing just right now for example
    // I don't know if watching multiple times is a problem
    val watcherAttempt = Try {
      context.watch(workerTypeInfo.actorRef)
    }

    watcherAttempt match {
      case Success(res) =>
      case Failure(err) => logger.error(s"Impossible to watch a WorkerTypeInfo: $workerTypeInfo")
    }

    watcherAttempt
  }

  /**
    * Watch the Actor having sent the WorkerTypeOrder, it might be different from the other Worker & Leader in some cases
    * @param context
    * @param workerTypeOrder
    * @return
    */
  def watchWorkerTypeOrder(context: ActorContext, workerTypeOrder: WorkerTypeOrder): Try[ActorRef] = {
    // A watch can fail if the actor is failing just right now for example
    // I don't know if watching multiple times is a problem
    val watcherAttempt = Try {
      context.watch(workerTypeOrder.actorRef)
    }

    watcherAttempt match {
      case Success(res) =>
      case Failure(err) => logger.error(s"Impossible to watch the sender of a WorkerTypeOrder: $workerTypeOrder")
    }

    watcherAttempt
  }

  /**
    * When a StartedWorkerActor is received, we want to update its status, and update the clusterTopology information
    * @param startedWorkerActor
    */
  def registerStartedWorkerActor(startedWorkerActor: StartedWorkerActor): Unit = {
    logger.debug(s"Register StartedWorkerActor: $startedWorkerActor (jvm id: ${startedWorkerActor.jvmId}). System was: \n${this}")

    // The WorkerActorHealth is received separately and it will be updated from time to time when we ask for an update
    // at each jvm. See updateWorkerActorHealth() which is called when we receive the appropriate message.

    // For whatever reason, we can receive twice the same message even if the worker is already started.
    val workerTypeInfo = WorkerTypeInfo.fromWorkerTypeInfo(startedWorkerActor.startWorkerActor.workerTypeInfo, startedWorkerActor.actorRef, startedWorkerActor.nodeInfo)
    Cluster.getWorkerFromWorkerId(this, startedWorkerActor.startWorkerActor.workerId, startedWorkerActor.jvmId) match {
      case Some(previousWorker) =>
        val worker = Cluster.getAndAddWorker(this, workerTypeInfo, startedWorkerActor.startWorkerActor.orderId, startedWorkerActor.startWorkerActor.workerId, Option(startedWorkerActor.runningActorRef))
        worker.setStatus(ClusterElementRunning())
      case None =>
        logger.error("We got a StartedWorkerActor which was already removed from the local topology. TODO: We should solve this bug")
    }
  }

  def updateWorkerActorHealth(clusterTopology: ClusterTopology, workerActorHealth: WorkerActorHealth): Unit = {
    clusterTopology.addWorkerActor(workerActorHealth) // Update if already existing
  }

  /**
    * When a FailedWorkerActor is received, we want to update its status. At the same time, we need to update the health
    * @param failedWorkerActor
    */
  def registerFailedWorkerActor(clusterTopology: ClusterTopology, failedWorkerActor: FailedWorkerActor): Unit = {
    val workerTypeInfo = WorkerTypeInfo.fromWorkerTypeInfo(failedWorkerActor.startWorkerActor.workerTypeInfo, failedWorkerActor.actorRef, failedWorkerActor.nodeInfo)
    val worker = Cluster.getAndAddWorker(this, workerTypeInfo, failedWorkerActor.startWorkerActor.orderId, failedWorkerActor.startWorkerActor.workerId, None)
    worker.setStatus(ClusterElementFailed(failedWorkerActor.exception))

    // We also need to update the ClusterTopology directly
    clusterTopology.removeWorkerActor(failedWorkerActor.actorRef)
  }

  /**
    * Registered a failed WorkerActor, typically because of a Terminated message. Return True if the actor was found, false
    * otherwise.
    */
  def registerFailedWorkerActor(actorRef: ActorRef): Boolean = {
    val worker = Cluster.getWorker(this, actorRef)
    worker match {
      case Some(work) =>
        work.setStatus(ClusterElementFailed(new TerminatedActor(s"$actorRef")))
        true
      case None =>
        logger.debug(s"Failed ActorRef $actorRef is not linked to a known WorkerActor. Maybe it's a WorkerLeader?")
        false
    }
  }

  /**
    * Kill all actors which were related to a specific workerTypeOrder
    * @param workerTypeOrderWrapper
    */
  private def killActorsForOrder(context: ActorContext, workerTypeOrderWrapper: WorkerTypeOrderWrapper): Unit = {
    // TODO: There is no retry attempt for this system, we expect all messages to go through
    val order = workerTypeOrderWrapper.order
    _nodes.flatMap(_.workerManagers)
        .flatMap(_.workerTypes.filter(_.workerTypeInfo.workerTypeId == order.workerTypeId))
        .flatMap(_.workers.filter(_.orderId == order.id))
        .foreach(worker => {
          logger.info(s"Asking the actor ${worker.actorRef} to kill itself as it belongs to WorkerTypeId ${order.workerTypeId} and we must all workers belonging to the orderId ${order.id}")
          worker.actorRef.foreach(actorRef => actorRef ! KillWorkerActor(context.self))
        })
  }

  /**
    * Check if the failed actor is linked to the initiator of a WorkerTypeOrder. If this is the case update the related
    * orders.
    * If the actorRef is linked to the initiator of a WorkerTypeOrder, remove it. If other sent the same order, no problem.
    * If no WorkerOrder is alive anymore (for the same id), the related workers will be directly removed.
    */
  def registerFailedWorkerOrderSender(context: ActorContext, actorRef: ActorRef): Unit = {
    _orders.filter(_._2.actors.remove(actorRef))
      .filter(_._2.actors.isEmpty)
      .map(raw => {
        logger.warn(s"There are no remaining initiators of the WorkerTypeOrder for WorkerTypeId ${raw._2.order.workerTypeId}, remove the order and ask for the killing of the related actors.")
        killActorsForOrder(context, raw._2)
        _orders.remove(raw._1)
      })
  }

  /**
    * Registered a failed WorkerLeader, typically because of a Terminated message. Return True if the actor was found, false
    * otherwise.
    * By having a failed WorkerLeader, it means that the status of any object beneath it is unknown, but we will most likely
    * also receive Terminated message and so on for each actor.
    */
  def registerFailedWorkerLeader(actorRef: ActorRef): Boolean = {
    // Update the general information for the topology
    val workerLeader = Cluster.getWorkerManager(this, actorRef)
    workerLeader match {
      case Some(workLeader) =>
        workLeader.setStatus(ClusterElementFailed(new TerminatedActor(s"$actorRef")))
        true
      case None =>
        logger.debug(s"Failed ActorRef $actorRef is not linked to a known WorkerActor. Maybe it's a WorkerLeader?")
        false
    }
  }

  /**
    * Every time a WorkerActor is started we want to monitor it
    * @param startedWorkerActor
    */
  def watchWorkerActor(context: ActorContext, startedWorkerActor: StartedWorkerActor): Try[ActorRef] = {
    val watcherAttempt = Try{
      context.watch(startedWorkerActor.runningActorRef)
    }

    watcherAttempt match {
      case Success(res) =>
      case Failure(err) => logger.error(s"Impossible to watch a WorkerActor: $startedWorkerActor")
    }

    watcherAttempt
  }

  /**
    * Every time a WorkerActor has failed we remove the monitoring
    * @param actorRef
    */
  def unwatchWorkerActor(context: ActorContext, actorRef: ActorRef): Try[ActorRef] = {
    val watcherAttempt = Try{
      context.unwatch(actorRef)
    }

    watcherAttempt match {
      case Success(res) =>
      case Failure(err) => logger.error(s"Impossible to unwatch the actorRef of a WorkerActor or WorkerLeader: $actorRef")
    }

    watcherAttempt
  }

  /**
    * Find the best node to start an Instance. For now we just select one at random
    * @return
    */
  def bestNodeForWorker(workerType: WorkerType): Option[Node] = {
    nodesByWorkerType().get(workerType) match {
      case Some(nodes) =>
        Random.shuffle(nodes.filter(_.workerManagers.nonEmpty)).headOption
      case None =>
        logger.error(s"Normally this method should only be called if there are existing nodes available for a workerType $workerType")
        None
    }
  }

  def nodesByWorkerType(): Map[WorkerType, Seq[Node]] = {
    nodes.flatMap(node => node.workerManagers.flatMap(_.workerTypes.map(step => (node, step))))
      .groupBy(_._2).map(raw => raw._1 -> raw._2.map(_._1))
  }

  def nodesForWorkerType(workerType: WorkerType): Seq[Node] = {
    nodesByWorkerType().getOrElse(workerType, List.empty[Node])
  }

  /**
    * Remove any worker not put as "running" after x seconds, also remove the "failed" workers.
    * New workers will be automatically started by another method, we don't deal with that here.
    */
  def cleanOldWorkers(): Unit = {
    val timeout = clusterConfiguration.startWorkerTimeout.toMillis
    nodes.flatMap(_.workerManagers).flatMap(_.workerTypes).foreach(_.cleanOldWorkers(timeout))
  }

  /**
    * Add a Node we received from another JVM, only if not already present. Return the inserted Node, or the previously
    * existing node
    * @param node
    */
  def addNode(node: Node): Node = {
    _nodes.find(_ == node) match {
      case Some(previousNode) => previousNode
      case None =>
        _nodes = _nodes :+ node
        node
    }
  }

  def nodes: Seq[Node] = _nodes

  /**
    * Reconstruct the structure of Cluster and its objects based on a given ClusterTopology.
    */
  def reconstructFromClusterTopology(clusterTopology: ClusterTopology): Unit = {
    // Reconstruct every node
    _nodes = List()

    clusterTopology.workerActors.values.flatten.map(workerActorHealth => {
      val rawNode = Node.fromNodeInfo(workerActorHealth.nodeInfo)
      addNode(rawNode)
    })

    // For every node, we reconstruct the WorkerManagers
    nodes.foreach(_.reconstructFromClusterTopology(clusterTopology))
  }

  override def toString: String = {
    // WorkerTypeId -> List(orders)
    val orders: Map[String, Seq[WorkerTypeOrderWrapper]] = _orders.groupBy(_._2.order.workerTypeId).map(raw => raw._1 -> raw._2.values.toList)

    nodes.flatMap(node => {
      val nodeText: String = s"Node: ${node.hostname}"
      node.workerManagers.map(workerManager => {
        val workerManagerText: String = s"$nodeText - port: ${workerManager.port}"
        val workerTypeTexts: String = workerManager.workerTypes.map(workerType => {
          // Go through every order for the given workerType
          val prettyOrders = orders.getOrElse(workerType.workerTypeInfo.workerTypeId, List.empty[WorkerTypeOrderWrapper])
              .map(order => {
                s"${order.order.loadBalancerType}"
              }).mkString(", ")

          val workerTypeText: String = s"\t${workerType.id} - orders: ${prettyOrders} -"
          val workerTexts = workerType.workers.map(worker => worker.status.toString).groupBy(status => status).map(status => s"${status._1}: ${status._2.size}").mkString(", ")
          s"$workerTypeText $workerTexts"
        }).mkString("\n")

        s"$workerManagerText\n$workerTypeTexts"
      }).mkString("\n")
    }).mkString("")
  }
}

object Cluster {
  def getAndAddNode(cluster: Cluster, workerTypeInfo: WorkerTypeInfo): Node = {
    val rawNode = Node.fromNodeInfo(workerTypeInfo.nodeInfo)
    cluster.addNode(rawNode)
  }

  def getNode(cluster: Cluster, actorRef: ActorRef): Option[Node] = {
    // We go through each level, while keeping the top node between each step.
    cluster.nodes.flatMap(node => node.workerManagers.map(workerManager => (node, workerManager)))
      .flatMap(obj => obj._2.workerTypes.map(workerType => (obj._1, workerType)))
      .flatMap(obj => obj._2.workers.map(worker => (obj._1, worker)))
      .find(obj => obj._2.actorRef.isDefined && obj._2.actorRef.get == actorRef)
      .map(_._1)
  }

  def getAndAddWorkerManager(cluster: Cluster, workerTypeInfo: WorkerTypeInfo): WorkerManager = {
    val node = Cluster.getAndAddNode(cluster, workerTypeInfo)
    val rawWorkerManager = WorkerManager.fromWorkerTypeInfo(workerTypeInfo)
    node.addWorkerManager(rawWorkerManager)
  }

  def getWorkerManager(cluster: Cluster, actorRef: ActorRef): Option[WorkerManager] = {
    cluster.nodes.flatMap(_.workerManagers)
      .flatMap(obj => obj.workerTypes.map(workerType => (obj, workerType)))
      .flatMap(obj => obj._2.workers.map(worker => (obj._1, worker)))
      .find(obj => obj._2.actorRef.isDefined && obj._2.actorRef.get == actorRef)
      .map(_._1)
  }

  def getAndAddWorkerType(cluster: Cluster, workerTypeInfo: WorkerTypeInfo): WorkerType = {
    val workerManager = Cluster.getAndAddWorkerManager(cluster, workerTypeInfo)
    val rawWorkerType = WorkerType.fromWorkerTypeInfo(workerTypeInfo)
    workerManager.addWorkerType(rawWorkerType)
  }

  def getWorkerType(cluster: Cluster, actorRef: ActorRef): Option[WorkerType] = {
    cluster.nodes.flatMap(_.workerManagers).flatMap(_.workerTypes)
      .flatMap(obj => obj.workers.map(worker => (obj, worker)))
      .find(obj => obj._2.actorRef.isDefined && obj._2.actorRef.get == actorRef)
      .map(_._1)
  }

  /**
    * Be careful to NOT use the workerTypeInfo from startedWorkerActor to get the jvmId, as the jvmid inside it is not related to the actor
    * starting the job, so you will have strange behavior!
    * @return
    */
  def getAndAddWorker(cluster: Cluster, workerTypeInfo: WorkerTypeInfo, orderId: String, workerId: String, workerActorRef: Option[ActorRef]): Worker = {
    val workerType = Cluster.getAndAddWorkerType(cluster, workerTypeInfo)
    val rawWorker = Worker.fromWorkerIdAndActorRef(workerId, orderId, workerActorRef)
    workerType.addWorker(rawWorker, replace = true)
  }

  def getWorker(cluster: Cluster, actorRef: ActorRef): Option[Worker] = {
    cluster.nodes.flatMap(_.workerManagers).flatMap(_.workerTypes).flatMap(_.workers)
      .find(worker => worker.actorRef.isDefined && worker.actorRef.get == actorRef)
  }

  def getWorkerFromWorkerId(cluster: Cluster, workerId: String, jvmId: String): Option[Worker] = {
    cluster.nodes.flatMap(_.workerManagers).filter(_.id == jvmId).flatMap(_.workerTypes).flatMap(_.workers)
      .find(worker => worker.workerId == workerId)
  }
}