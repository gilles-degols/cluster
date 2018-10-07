package net.degols.filesgate.libs.cluster.core

import javax.inject.Singleton
import org.slf4j.{Logger, LoggerFactory}
import scala.util.Random

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
    * When we receive an InstanceType, we need to save the related information locally, which include the related
    * node found (might be new), the InstanceTypeId, ...
    */
  def registerInstanceType(rawNode: Node, rawWorkerManager: WorkerManager, rawWorkerType: WorkerType): Unit = {
    val node = addNode(rawNode)
    val clusterInstance = node.addWorkerManager(rawWorkerManager)
    clusterInstance.addWorkerType(rawWorkerType)
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
}
