package net.degols.filesgate.libs.cluster.messages

case object IAmTheWorkerLeader

class ClusterException(val message: String) extends Exception(message)

class TerminatedActor(message: String) extends ClusterException(message)

case class DistributeWork(soft: Boolean)
case class CleanOldWorkers()