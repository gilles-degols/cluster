package net.degols.libs.cluster.manager

import net.degols.libs.cluster.balancing.LoadBalancer
import net.degols.libs.cluster.utils.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * The developer needs to implement this interface to be automatically injected by the ClusterWorkerLeader.
  * This Component contains general information about the available Component & their WorkerInfo. It is also
  * used to interact with the manager to send WorkerOrder
  */
trait ComponentLeaderApi extends Logging{
  /**
    * Component name. Must be unique accross all jvms.
    * We do not put a default value as after creating a second project, it might cause trouble if the developer forgot
    * to put a ComponentName
    * @return
    */
  val componentName: String

  /**
    * Link to all package leaders
    * @return
    */
  def packageLeaders: Future[Seq[PackageLeaderApi]]

  /**
    * Custom User LoadBalancer, they do not need to exist, it's just for advanced users. A reference to their instances
    * will be sent to local manager only (so they do not need to be serializable)
    */
  def loadBalancers: Future[Seq[LoadBalancer]] = Future{List.empty[LoadBalancer]}
}

/**
  * This class should not be exposed outside of the library as the developer might want to directly inject it, thinking
  * t's the same as ComponentLeaderApi. To avoid any misunderstanding, we make it private.
  */
private[cluster] case class ComponentLeader(
                     componentName: String,
                     packageLeaders: Seq[PackageLeaderApi],
                     loadBalancer: Seq[LoadBalancer]
                     )

object ComponentLeader {
  def from(componentLeaderApi: ComponentLeaderApi): Future[ComponentLeader] = {
    for {
      packageLeaders <- componentLeaderApi.packageLeaders
      loadBalancers <- componentLeaderApi.loadBalancers
    } yield {
      ComponentLeader(componentLeaderApi.componentName, packageLeaders, loadBalancers)
    }
  }
}