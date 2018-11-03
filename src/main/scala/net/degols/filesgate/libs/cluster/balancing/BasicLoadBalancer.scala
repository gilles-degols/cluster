package net.degols.filesgate.libs.cluster.balancing

import akka.actor.ActorContext
import net.degols.filesgate.libs.cluster.core.WorkerType
import net.degols.filesgate.libs.cluster.messages._
import org.slf4j.LoggerFactory

/**
  * Very basic load balancing: Simply try to create the expected number of actors, nothing more, nothing less. No real
  * load balancing in fact.
  * We can have multiple load balancing working together, but only one will be used for a given WorkerType
  */
class BasicLoadBalancer extends LoadBalancer {
  private val logger = LoggerFactory.getLogger(getClass)

  override def isLoadBalancerType(loadBalancerType: LoadBalancerType): Boolean = loadBalancerType.isInstanceOf[BasicLoadBalancerType]

  override def hardWorkDistribution(workerType: WorkerType): Unit = {
    logger.error("Not yet implemented")
  }

  def softWorkDistribution(workerType: WorkerType): Unit = {
    val nodes = clusterManagement.cluster.nodesForWorkerType(workerType)
    if(nodes.isEmpty) {
      logger.error(s"The WorkerType $workerType has no nodes available, no work distribution possible.")
    } else {
      // Depending on the type of WorkType, we want to create a specific number of workers by JVM
      if(workerType.workerTypeInfo.instanceType == JVMInstance) {
        nodes.flatMap(_.workerManagers)
          .map(workerManager => {

          })
      } else {

      }
      nodes.flatMap(_.workerManagers).sortBy(_.workerTypes.map(_.workers.filter(worker => )))
    }
  }
}
