package net.degols.filesgate.libs.cluster.core

import net.degols.filesgate.libs.cluster.Tools
import net.degols.filesgate.libs.cluster.messages.{ClusterTopology, InstanceType, NodeInfo, WorkerTypeInfo}

/**
  * A WorkerType manages all worker instances of the same type as itself.
  *
  * @param id
  */
class WorkerType(val id: String, workerTypeInfo: WorkerTypeInfo) {
  private var _workers: List[Worker] = List.empty[Worker]

  def addWorker(rawWorker: Worker): Worker = {
    _workers.find(_ == rawWorker) match {
      case Some(previousWorker) => previousWorker
      case None =>
        _workers = _workers :+ rawWorker
        rawWorker
    }
  }

  def workers: List[Worker] = _workers

  def canEqual(a: Any): Boolean = a.isInstanceOf[WorkerType]
  override def equals(that: Any): Boolean =
    that match {
      case that: WorkerType => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  override def hashCode: Int = s"WorkerType:$id".hashCode

  def reconstructFromClusterTopology(clusterTopology: ClusterTopology, currentNode: Node, currentWorkerManager: WorkerManager): Unit = {
    _workers = List.empty[Worker]

    clusterTopology.workerActors.values.flatten.filter(workerActorHealth => {
      val rawNode = Node.fromNodeInfo(workerActorHealth.nodeInfo)
      // We only keep WorkerManagers for the current node
      rawNode == currentNode
    }).filter(workerActorHealth => {
      // We only keep the current WorkerManager
      val rawWorkerManager = new WorkerManager(workerActorHealth.jvmId, workerActorHealth.workerLeader)
      rawWorkerManager == currentWorkerManager
    }).filter(workerActorHealth => {
      // We only keep the current WorkerType
      val rawWorkerType = new WorkerType(workerActorHealth.workerTypeId, workerActorHealth.workerTypeInfo)
      rawWorkerType == this
    }).foreach(workerActorHealth => {
      val rawWorker = new Worker(workerActorHealth.workerActorId, Option(workerActorHealth.workerActorRef))
      addWorker(rawWorker)
    })

    // No need to go one step below, there is nothing to re-construct on the Worker level
  }
}

object WorkerType {
  def fromWorkerTypeInfo(workerTypeInfo: WorkerTypeInfo): WorkerType = {
    val workerType = new WorkerType(workerTypeInfo.workerTypeId, workerTypeInfo)
    workerType
  }
}