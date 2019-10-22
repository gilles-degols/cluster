package net.degols.libs.cluster.core

import net.degols.libs.cluster.ClusterTools
import net.degols.libs.cluster.messages.{ClusterTopology, InstanceType, NodeInfo, WorkerTypeInfo}

/**
  * A WorkerType manages all worker instances of the same type as itself.
  *
  * @param id
  */
class WorkerType(val id: String, val workerTypeInfo: WorkerTypeInfo) {
  private var _workers: Seq[Worker] = List.empty[Worker]

  /**
    * Remove any worker not put as "running" after x seconds, also remove the "failed" workers.
    * New workers will be automatically started by another method, we don't deal with that here. This is generally called
    * by the Cluster only (to avoid any external call from a developer).
    */
  private[core] def cleanOldWorkers(timeout: Long): Unit = {
    // Remove failed workers (not the "paused" one)
    _workers = _workers.filterNot(worker => worker.isFailed)
    // Remove worker stucked in the "started" state for too long
    _workers = _workers.filterNot(worker => worker.isStarting && Math.abs(ClusterTools.difference(worker.status.creationDatetime)) > timeout)
  }

  def addWorker(rawWorker: Worker, replace: Boolean = false): Worker = {
    _workers.find(_ == rawWorker) match {
      case Some(previousWorker) =>
        if(replace) {
          _workers = _workers.filterNot(_ == previousWorker) :+ rawWorker // Even if the objects are "equal" they can share differences in their attributes
          rawWorker
        } else {
          previousWorker
        }
      case None =>
        _workers = _workers :+ rawWorker
        rawWorker
    }
  }

  def workers: Seq[Worker] = _workers

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
      val orderId = workerActorHealth.workerTypeOrder.id
      val rawWorker = new Worker(workerActorHealth.workerActorId, orderId, Option(workerActorHealth.workerActorRef))
      addWorker(rawWorker)
    })

    // No need to go one step below, there is nothing to re-construct on the Worker level
  }

  override def toString: String = {
    s"WorkerType($id, $workerTypeInfo)"
  }
}

object WorkerType {
  def fromWorkerTypeInfo(workerTypeInfo: WorkerTypeInfo): WorkerType = {
    val workerType = new WorkerType(workerTypeInfo.workerTypeId, workerTypeInfo)
    workerType
  }
}