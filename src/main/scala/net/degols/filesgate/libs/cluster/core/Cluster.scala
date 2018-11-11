package net.degols.filesgate.libs.cluster.core

import javax.inject.Singleton

import akka.actor.{ActorContext, ActorRef}
import com.google.inject.Inject
import net.degols.filesgate.libs.cluster.ClusterConfiguration
import net.degols.filesgate.libs.cluster.messages._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Random, Success, Try}

/**
  * General interface to access information about the cluster
  */
@Singleton
class Cluster @Inject()(clusterConfiguration: ClusterConfiguration) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * All instances of Nodes found on the cluster
    */
  private var _nodes: List[Node] = List.empty[Node]

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
    * When a StartedWorkerActor is received, we want to update its status
    * @param startedWorkerActor
    */
  def registerStartedWorkerActor(startedWorkerActor: StartedWorkerActor): Unit = {
    logger.debug(s"Register StartedWorkerActor: $startedWorkerActor (jvm id: ${startedWorkerActor.jvmId}). System was: \n${this}")
    // For whatever reason, we can receive twice the same message even if the worker is already started.
    val workerTypeInfo = WorkerTypeInfo.fromWorkerTypeInfo(startedWorkerActor.startWorkerActor.workerTypeInfo, startedWorkerActor.actorRef, startedWorkerActor.nodeInfo)
    Cluster.getWorkerFromWorkerId(this, startedWorkerActor.startWorkerActor.workerId, startedWorkerActor.jvmId) match {
      case Some(previousWorker) =>
        val worker = Cluster.getAndAddWorker(this, workerTypeInfo, startedWorkerActor.startWorkerActor.workerId, Option(startedWorkerActor.runningActorRef))
        worker.setStatus(ClusterElementRunning())
      case None =>
        logger.error("We got a StartedWorkerActor which was already removed from the local topology. TODO: We should solve this bug")
    }
  }

  /**
    * When a FailedWorkerActor is received, we want to update its status
    * @param failedWorkerActor
    */
  def registerFailedWorkerActor(failedWorkerActor: FailedWorkerActor): Unit = {
    val workerTypeInfo = WorkerTypeInfo.fromWorkerTypeInfo(failedWorkerActor.startWorkerActor.workerTypeInfo, failedWorkerActor.actorRef, failedWorkerActor.nodeInfo)
    val worker = Cluster.getAndAddWorker(this, workerTypeInfo, failedWorkerActor.startWorkerActor.workerId, None)
    worker.setStatus(ClusterElementFailed(failedWorkerActor.exception))
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
        logger.debug("Failed ActorRef is not linked to a known WorkerActor. Maybe it's a WorkerLeader?")
        false
    }
  }

  /**
    * Registered a failed WorkerLeader, typically because of a Terminated message. Return True if the actor was found, false
    * otherwise.
    * By having a failed WorkerLeader, it means that the status of any object beneath it is unknown, but we will most likely
    * also receive Terminated message and so on for each actor.
    */
  def registerFailedWorkerLeader(actorRef: ActorRef): Boolean = {
    val workerLeader = Cluster.getWorkerManager(this, actorRef)
    workerLeader match {
      case Some(workLeader) =>
        workLeader.setStatus(ClusterElementFailed(new TerminatedActor(s"$actorRef")))
        true
      case None =>
        logger.debug("Failed ActorRef is not linked to a known WorkerActor. Maybe it's a WorkerLeader?")
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

  def nodesByWorkerType(): Map[WorkerType, List[Node]] = {
    nodes.flatMap(node => node.workerManagers.flatMap(_.workerTypes.map(step => (node, step))))
      .groupBy(_._2).map(raw => raw._1 -> raw._2.map(_._1))
  }

  def nodesForWorkerType(workerType: WorkerType): List[Node] = {
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

  def nodes: List[Node] = _nodes

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
    nodes.flatMap(node => {
      val nodeText: String = s"Node: ${node.hostname}"
      node.workerManagers.map(workerManager => {
        val workerManagerText: String = s"$nodeText - port: ${workerManager.port}"
        val workerTypeTexts: String = workerManager.workerTypes.map(workerType => {
          val workerTypeText: String = s"\t${workerType.id} - ${workerType.workerTypeInfo.loadBalancerType} -"
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
  def getAndAddWorker(cluster: Cluster, workerTypeInfo: WorkerTypeInfo,  workerId: String, workerActorRef: Option[ActorRef]): Worker = {
    val workerType = Cluster.getAndAddWorkerType(cluster, workerTypeInfo)
    val rawWorker = Worker.fromWorkerIdAndActorRef(workerId, workerActorRef)
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