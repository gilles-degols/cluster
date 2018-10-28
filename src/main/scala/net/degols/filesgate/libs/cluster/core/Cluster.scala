package net.degols.filesgate.libs.cluster.core

import javax.inject.Singleton

import akka.actor.{ActorContext, ActorRef}
import net.degols.filesgate.libs.cluster.messages._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Random, Success, Try}

/**
  * General interface to access information about the cluster
  */
@Singleton
class Cluster {
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
  def watchWorkerTypeInfo(context: ActorContext, workerTypeInfo: WorkerTypeInfo): Try[Unit] = {
    // A watch can fail if the actor is failing just right now for example
    // I don't know if watching multiple times is a problem
    Try{
      context.watch(workerTypeInfo.actorRef)
    } match {
      case Success(res) => Try{res}
      case Failure(err) =>
        logger.error(s"Impossible to watch a WorkerTypeInfo: $workerTypeInfo")
        Try{err}
    }
  }

  /**
    * When a StartedWorkerActor is received, we want to update its status
    * @param startedWorkerActor
    */
  def registerStartedWorkerActor(startedWorkerActor: StartedWorkerActor): Unit = {
    val worker = Cluster.getWorker(this, startedWorkerActor.startWorkerActor.workerTypeInfo, startedWorkerActor.startWorkerActor.workerId, Option(startedWorkerActor.runningActorRef))
    worker.running = true
  }

  /**
    * When a FailedWorkerActor is received, we want to update its status
    * @param failedWorkerActor
    */
  def registerFailedWorkerActor(failedWorkerActor: FailedWorkerActor): Unit = {
    val worker = Cluster.getWorker(this, failedWorkerActor.startWorkerActor.workerTypeInfo, failedWorkerActor.startWorkerActor.workerId, None)
    worker.running = true
  }

  /**
    * Every time a WorkerActor is started we want to monitor it
    * @param startedWorkerActor
    */
  def watchWorkerActor(context: ActorContext, startedWorkerActor: StartedWorkerActor): Try[Unit] = {
    Try{
      context.watch(startedWorkerActor.runningActorRef)
    } match {
      case Success(res) => Try{res}
      case Failure(err) =>
        logger.error(s"Impossible to watch a WorkerActor: $startedWorkerActor")
        Try{err}
    }
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
}

object Cluster {
  def getNode(cluster: Cluster, workerTypeInfo: WorkerTypeInfo): Node = {
    val rawNode = Node.fromNodeInfo(workerTypeInfo.nodeInfo)
    cluster.addNode(rawNode)
  }

  def getWorkerManager(cluster: Cluster, workerTypeInfo: WorkerTypeInfo): WorkerManager = {
    val node = Cluster.getNode(cluster, workerTypeInfo)
    val rawWorkerManager = WorkerManager.fromWorkerTypeInfo(workerTypeInfo)
    node.addWorkerManager(rawWorkerManager)
  }

  def getWorkerType(cluster: Cluster, workerTypeInfo: WorkerTypeInfo): WorkerType = {
    val workerManager = Cluster.getWorkerManager(cluster, workerTypeInfo)
    val rawWorkerType = WorkerType.fromWorkerTypeInfo(workerTypeInfo)
    workerManager.addWorkerType(rawWorkerType)
  }

  def getWorker(cluster: Cluster, workerTypeInfo: WorkerTypeInfo, workerId: String, workerActorRef: Option[ActorRef]): Worker = {
    val workerType = Cluster.getWorkerType(cluster, workerTypeInfo)
    val rawWorker = Worker.fromWorkerIdAndActorRef(workerId, workerActorRef)
    workerType.addWorker(rawWorker)
  }
}