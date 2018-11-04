package net.degols.filesgate.libs.cluster.core

import org.joda.time.{DateTime, DateTimeZone}

/**
  * Each Cluster Element (Cluster, Worker, etc.) can only have one status at each time
  */
trait ClusterElementStatus{
  val creationDatetime: DateTime = new DateTime().withZone(DateTimeZone.UTC)
}

case class ClusterElementRunning() extends ClusterElementStatus()
case class ClusterElementStarting() extends ClusterElementStatus() // No acknowledgment received yet
abstract class ClusterElementStopped() extends ClusterElementStatus()
// Special state when we don't really know the status. This is a temporary state when we have just created an object.
case class ClusterElementUnknown() extends ClusterElementStopped()


case class ClusterElementFailed(exception: Exception) extends ClusterElementStopped()
case class ClusterElementPaused() extends ClusterElementStopped() // A Paused worker can be restarted afterwards

