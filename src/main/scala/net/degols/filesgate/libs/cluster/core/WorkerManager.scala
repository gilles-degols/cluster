package net.degols.filesgate.libs.cluster.core

import akka.actor.ActorRef
import net.degols.filesgate.libs.cluster.messages.{ClusterTopology, NodeInfo, WorkerTypeInfo}

/**
  * We could have multiple instances of WorkerManager running on the same machine. So the id is concatenation of
  * the hostname and a unique string gathered by the remote process (the PID for example). There is one WorkerManager
  * per JVM, it will handle multiple WorkerTypes.
  *
  * @param id
  */
class WorkerManager(id: String, val actorRef: ActorRef) {
  /**
    * All pipeline steps of the running system
    */
  private var _workerTypes: List[WorkerType] = List.empty[WorkerType]

  /**
    * Add a worker type we received from another JVM, if it does not exist yet
    * @param rawWorkerType
    */
  def addWorkerType(rawWorkerType: WorkerType): WorkerType = {
    _workerTypes.find(_ == rawWorkerType) match {
      case Some(previousWorkerType) => previousWorkerType
      case None =>
        _workerTypes = _workerTypes :+ rawWorkerType
        rawWorkerType
    }
  }

  /**
    * Return the pipeline steps we have
    */
  def workerTypes: List[WorkerType] = _workerTypes

  def canEqual(a: Any): Boolean = a.isInstanceOf[WorkerManager]
  override def equals(that: Any): Boolean =
    that match {
      case that: WorkerManager => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  override def hashCode: Int = s"WorkerManager:$id".hashCode

  def reconstructFromClusterTopology(clusterTopology: ClusterTopology, currentNode: Node): Unit = {
    _workerTypes = List.empty[WorkerType]

    clusterTopology.workerActors.values.flatten.filter(workerActorHealth => {
      val rawNode = Node.fromNodeInfo(workerActorHealth.nodeInfo)
      // We only keep WorkerManagers for the current node
      rawNode == currentNode
    }).filter(workerActorHealth => {
      // We only keep the current WorkerManager
      val rawWorkerManager = new WorkerManager(workerActorHealth.jvmId, workerActorHealth.workerLeader)
      rawWorkerManager == this
    }).foreach(workerActorHealth => {
      val rawWorkerType = new WorkerType(workerActorHealth.workerTypeId, workerActorHealth.workerTypeInfo)
      addWorkerType(rawWorkerType)
    })

    workerTypes.foreach(workerType => workerType.reconstructFromClusterTopology(clusterTopology, currentNode, this))
  }
}

object WorkerManager {
  def fromWorkerTypeInfo(workerTypeInfo: WorkerTypeInfo): WorkerManager = {
    val workerManager = new WorkerManager(workerTypeInfo.jvmId, workerTypeInfo.actorRef) // WorkerTypeInfo sent by WorkerLeader, so the actor ref is ok
    workerManager
  }
}