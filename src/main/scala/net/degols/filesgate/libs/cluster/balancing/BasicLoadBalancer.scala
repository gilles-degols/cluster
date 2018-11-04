package net.degols.filesgate.libs.cluster.balancing

import akka.actor.ActorContext
import net.degols.filesgate.libs.cluster.core.{Node, Worker, WorkerManager, WorkerType}
import net.degols.filesgate.libs.cluster.messages._
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * Very basic load balancing: Simply try to create the expected number of actors, nothing more, nothing less. No real
  * load balancing in fact.
  * We can have multiple load balancing working together, but only one will be used for a given WorkerType
  */
class BasicLoadBalancer extends LoadBalancer {
  private val logger = LoggerFactory.getLogger(getClass)

  override def isLoadBalancerType(loadBalancerType: LoadBalancerType): Boolean = loadBalancerType.isInstanceOf[BasicLoadBalancerType]

  override def hardWorkDistribution(workerType: WorkerType): Unit = {
    logger.error("There is no hard work distribution in the BasicLoadBalancer.")
  }

  def softWorkDistribution(workerType: WorkerType): Unit = {
    val nodes = clusterManagement.cluster.nodesForWorkerType(workerType)
    val balancerType = workerType.workerTypeInfo.loadBalancerType.asInstanceOf[BasicLoadBalancerType]

    if(nodes.isEmpty) {
      logger.error(s"The WorkerType $workerType has no nodes available, no work distribution possible.")
    } else {
      // Depending on the type of WorkType, we want to create a specific number of workers by JVM or per cluster
      if(balancerType.instanceType == JVMInstance) {
        softWorkDistributionPerJVM(workerType, nodes, balancerType)
      } else {
        softWorkDistributionPerCluster(workerType, nodes, balancerType)
      }
    }
  }

  private def softWorkDistributionPerJVM(workerType: WorkerType, nodes: List[Node], balancerType: BasicLoadBalancerType): Unit = {
    val wantedInstances = balancerType.instances

    nodes.flatMap(_.workerManagers.filterNot(_.isStopped))
      .foreach(workerManager => {
        val runningInstances = workerManager.workerTypes.find(_ == workerType).get.workers.filterNot(_.isStopped)

        var i = runningInstances.size
        if(i < wantedInstances) {
          logger.info(s"Starting ${wantedInstances - i} instances of $workerType on $this")
        }
        while(i < wantedInstances) {
          workerManager.startWorker(context, workerType)
          i += 1
        }
      })
  }

  private def softWorkDistributionPerCluster(workerType: WorkerType, nodes: List[Node], balancerType: BasicLoadBalancerType): Unit = {
    val wantedInstances = balancerType.instances

    val managerAndRunningInstances: Map[WorkerManager, List[Worker]] = nodes.flatMap(node => node.workerManagers.filterNot(_.isStopped))
      .map(workerManager => workerManager -> workerManager.workerTypes.filter(_ == workerType).flatMap(_.workers.filterNot(_.isStopped))).toMap
    val runningInstances = managerAndRunningInstances.values.flatten.size
    if(managerAndRunningInstances.keys.isEmpty) {
      logger.warn(s"There is no WorkerManager available for $workerType, not possible to start the missing ${wantedInstances - wantedInstances} instances.")
    } else if(runningInstances < wantedInstances) {
      logger.info(s"Starting ${wantedInstances - runningInstances} instances of $workerType on various WorkerManagers.")
      // We try to distribute the load between managers. For now we simply choose managers at random (but those having less than the average number of instances)
      val averageWantedInstances = (wantedInstances + 1f) / managerAndRunningInstances.keys.size
      val availableManagers = managerAndRunningInstances.toList.filter(_._2.size < averageWantedInstances)
      if(availableManagers.isEmpty) {
        logger.error(s"No good WorkerManager found for $workerType...")
      } else {
        var i = runningInstances
        while(i < wantedInstances) {
          val workerManager = Random.shuffle(availableManagers).head
          workerManager._1.startWorker(context, workerType)
          i += 1
        }
      }
    }
  }

}
