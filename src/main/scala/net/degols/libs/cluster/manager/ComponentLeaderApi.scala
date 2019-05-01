package net.degols.libs.cluster.manager

import net.degols.libs.cluster.balancing.LoadBalancer

/**
  * The developer needs to implement this interface to be automatically injected by the ClusterWorkerLeader.
  * This Component contains general information about the available Component & their WorkerInfo. It is also
  * used to interact with the manager to send WorkerOrder
  */
trait ComponentLeaderApi {
  /**
    * Component name. Must be unique accross all jvms
    * @return
    */
  def componentName: String

  /**
    * Link to all package leaders
    * @return
    */
  def packageLeaders: List[PackageLeaderApi]

  /**
    * Custom User LoadBalancer, they do not need to exist, it's just for advanced users. A reference to their instances
    * will be sent to local manager only (so they do not need to be serializable)
    */
  def loadBalancers: List[LoadBalancer] = List.empty[LoadBalancer]
}
