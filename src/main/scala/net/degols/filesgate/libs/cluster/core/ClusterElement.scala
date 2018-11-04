package net.degols.filesgate.libs.cluster.core


/**
  * Not all objects of the Cluster need to extends this class, only those who could be enhanced with it. Typically
  * the classes directly monitored through Akka (WorkerLeader and Worker)
  */
trait ClusterElement{
  private var _statusHistory: List[ClusterElementStatus] = List(ClusterElementUnknown())

  def isUp: Boolean = !isDown
  def isDown: Boolean = isFailed

  def isRunning: Boolean = status.isInstanceOf[ClusterElementRunning]
  def isStarting: Boolean = status.isInstanceOf[ClusterElementStarting]
  def isStopped: Boolean = status.isInstanceOf[ClusterElementStopped]
  def isFailed: Boolean = status.isInstanceOf[ClusterElementFailed]
  def isPaused: Boolean = status.isInstanceOf[ClusterElementPaused]
  def isUnknown: Boolean = status.isInstanceOf[ClusterElementUnknown]

  def status: ClusterElementStatus = _statusHistory.last

  def setStatus(clusterElementStatus: ClusterElementStatus): Unit = {
    if(_statusHistory.size > 100) {
      _statusHistory = _statusHistory.drop(20)
    }
    _statusHistory = _statusHistory :+ clusterElementStatus
  }

  def statusHistory(): List[ClusterElementStatus] = _statusHistory
}
