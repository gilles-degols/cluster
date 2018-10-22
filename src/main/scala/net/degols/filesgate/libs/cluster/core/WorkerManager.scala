package net.degols.filesgate.libs.cluster.core

import akka.actor.ActorRef
import net.degols.filesgate.libs.cluster.messages.ClusterTopology

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
      case Some(previousPipelineStep) => previousPipelineStep
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

  def reconstructFromClusterTopology(clusterTopology: ClusterTopology): Unit = {
    // A CONTINUER !!!
  }
}