package net.degols.filesgate.libs.cluster.core

import net.degols.filesgate.libs.cluster.messages.{ClusterTopology, NodeInfo}


/**
  * Every information related to a Node. If there are multiple JVMs running on the same server, they will be linked to the same Node.
  * A WorkerManager is similar to a WorkerLeader. A workerManager != Manager
  */
class Node(val hostname: String) {
  /**
    * Local hostname seen by the process, which is different than the one used to contact it through the network
    */
  var localHostname: String = "Unknown"

  /**
    * All instances of WorkerManager found on the cluster
    */
  private var _workerManagers: List[WorkerManager] = List.empty[WorkerManager]

  /**
    * Add a workerManager we received from another JVM, only if not already present. Return the inserted WorkerManager, or the previously
    * existing WorkerManager
    * @param rawWorkerManager
    */
  def addWorkerManager(rawWorkerManager: WorkerManager): WorkerManager = {
    _workerManagers.find(_ == rawWorkerManager) match {
      case Some(previousWorkerManager) => previousWorkerManager
      case None =>
        _workerManagers = _workerManagers :+ rawWorkerManager
        rawWorkerManager
    }
  }

  def workerManagers: List[WorkerManager] = _workerManagers


  def canEqual(a: Any): Boolean = a.isInstanceOf[Node]
  override def equals(that: Any): Boolean =
    that match {
      case that: Node => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  override def hashCode: Int = s"Node:$hostname".hashCode


  def reconstructFromClusterTopology(clusterTopology: ClusterTopology): Unit = {
    _workerManagers = List.empty[WorkerManager]

    clusterTopology.workerActors.values.flatten.filter(workerActorHealth => {
      val rawNode = Node.fromNodeInfo(workerActorHealth.nodeInfo)
      // We only keep WorkerManagers for the current node
      rawNode == this
    }).foreach(workerActorHealth => {
      val rawWorkerManager = new WorkerManager(workerActorHealth.jvmId, workerActorHealth.workerLeader)
      addWorkerManager(rawWorkerManager)
    })

    workerManagers.foreach(_.reconstructFromClusterTopology(clusterTopology))
  }
}

object Node {
  def fromNodeInfo(nodeInfo: NodeInfo): Node = {
    val node = new Node(nodeInfo.networkHostname)
    node.localHostname = nodeInfo.localHostname
    node
  }
}