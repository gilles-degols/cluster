package net.degols.libs.cluster.messages

import net.degols.libs.cluster.balancing.LoadBalancer

case class IAmTheWorkerLeader(userLoadBalancers: List[LoadBalancer])

class ClusterException(val message: String) extends Exception(message)

class TerminatedActor(message: String) extends ClusterException(message)

case class DistributeWork(soft: Boolean)
case class CleanOldWorkers()