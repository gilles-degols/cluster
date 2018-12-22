package net.degols.libs.cluster.core

import org.joda.time.{DateTime, DateTimeZone}

/**
  * Each Cluster Element (Cluster, Worker, etc.) can only have one status at each time
  */
trait ClusterElementStatus{
  val creationDatetime: DateTime = new DateTime().withZone(DateTimeZone.UTC)
}

case class ClusterElementRunning() extends ClusterElementStatus() {override def toString: String = "Running"}
case class ClusterElementStarting() extends ClusterElementStatus() {override def toString: String = "Starting"} // No acknowledgment received yet
abstract class ClusterElementStopped() extends ClusterElementStatus()
// Special state when we don't really know the status. This is a temporary state when we have just created an object.
case class ClusterElementUnknown() extends ClusterElementStopped() {override def toString: String = "Unknown"}


case class ClusterElementFailed(exception: Exception) extends ClusterElementStopped() {override def toString: String = "Failed"}
case class ClusterElementPaused() extends ClusterElementStopped() {override def toString: String = "Paused"} // A Paused worker can be restarted afterwards

