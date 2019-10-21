package net.degols.libs.cluster.configuration

import java.io.File

import com.google.inject.Inject
import com.typesafe.config.{Config, ConfigFactory}
import javax.inject.Singleton
import net.degols.libs.cluster.ClusterTools
import net.degols.libs.election.ConfigurationMerge
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by Gilles.Degols on 03-09-18.
  */
@Singleton
class DefaultClusterConfiguration @Inject()(val defaultConfig: Config) extends ClusterConfigurationApi with ConfigurationMerge {

  /**
    * Configuration for the cluster system. We merge multiple configuration files: One embedded, the other one from the project
    * using the cluster library
    */
  val clusterConfig: Config = config.getConfig("cluster")

  /**
    * General execution context to use in the system
    */
  override implicit val executionContext: ExecutionContext = {
    ExecutionContext.fromExecutor(
      new java.util.concurrent.ForkJoinPool(clusterConfig.getInt("execution-context.threads"))
    )
  }

  override lazy val localHostname: Future[String] = Future{
    ClusterTools.runCommand("hostname")
  }

  override lazy val watcherTimeoutBeforeSuicide: Future[FiniteDuration] = {
    Future{
      config.getInt("cluster.watcher-timeout-before-suicide-ms") millis
    }
  }

  override lazy val softWorkDistributionFrequency: Future[FiniteDuration] = {
    // TODO: Frequency should be given for each load balancer
    Future{
      config.getInt("cluster.soft-work-distribution-ms") millis
    }
  }

  override lazy val hardWorkDistributionFrequency: Future[FiniteDuration] = {
    Future{
      config.getInt("cluster.hard-work-distribution-ms") millis
    }
  }

  /**
    * How much time do we allow to start a WorkerOrder before considering as failing?
    */
  override val startWorkerTimeout: Future[FiniteDuration] = {
    Future {
      config.getInt("cluster.start-worker-timeout-ms") millis
    }
  }

  /**
    * How many entries of ClusterInfo can we keep in the cache?
    * Normally there is no need to keep track of 1000s of them
    */
  override val clusterInfoCacheSize: Future[Int] = {
    Future{
      config.getInt("cluster.cache.cluster-info-size")
    }
  }

  /**
    * How much time (seconds) should we keep a ClusterInfo entry
    */
  override val clusterInfoCacheTimeout: Future[Int] = {
    Future{
      config.getInt("cluster.cache.cluster-info-lifetime-s")
    }
  }

  /**
    * Maximum timeout allowed to ask for info to the manager
    */
  override val clusterInfoTimeout: Future[Int] = {
    Future{
      config.getInt("cluster.cluster-info-timeout-s")
    }
  }

  /**
    * It's difficult to get a remote actor path locally. Because of that, we still want to know the current hostname + port
    */
  override val akkaLocalHostname: String = config.getString("akka.remote.netty.tcp.hostname")
  override val akkaLocalPort: Int = config.getInt("akka.remote.netty.tcp.port")

  /**
    * Methods to get data from the embedded configuration, or the project configuration (it can override it)
    */
  private def getStringList(path: String): Seq[String] = {
    config.getStringList(path).asScala.toList
  }

  override val directoryName: String = "cluster"
}