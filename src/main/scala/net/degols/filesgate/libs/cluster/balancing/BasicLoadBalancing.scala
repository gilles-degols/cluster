package net.degols.filesgate.libs.cluster.balancing

import akka.actor.ActorContext
import net.degols.filesgate.libs.cluster.messages.ClusterTopology
import org.slf4j.LoggerFactory

/**
  * Created by Gilles.Degols on 22-10-18.
  */
class BasicLoadBalancing(context: ActorContext) extends LoadBalancing(context) {

}
