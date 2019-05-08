package net.degols.libs.cluster.messages

import net.degols.libs.cluster.balancing.LoadBalancer

case class IAmTheWorkerLeader(userLoadBalancers: Seq[LoadBalancer])

class ClusterException(val message: String) extends Exception(message)

class TerminatedActor(message: String) extends ClusterException(message)
class UnknownWorker(message: String) extends ClusterException(message)
class MissingActor(message: String) extends ClusterException(message)

case class DistributeWork(soft: Boolean)
case class CleanOldWorkers()

/**
  * Message typically sent after an initial configuration has just been charged, to only start the real work.
  */
case object StartActor