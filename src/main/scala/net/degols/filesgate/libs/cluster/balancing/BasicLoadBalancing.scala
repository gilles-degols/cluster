package net.degols.filesgate.libs.cluster.balancing

import akka.actor.ActorContext
import net.degols.filesgate.libs.cluster.messages.ClusterTopology
import org.slf4j.LoggerFactory

/**
  * Very basic load balancing: Simply try to create the expected number of actors, nothing more, nothing less. No real
  * load balancing in fact.
  */
class BasicLoadBalancing extends LoadBalancing {

  override def hardWorkDistribution(): Unit = {

  }

  override def softWorkDistribution(): Unit = {

  }
}
