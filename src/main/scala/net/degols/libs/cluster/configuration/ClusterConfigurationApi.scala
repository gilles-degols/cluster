package net.degols.libs.cluster.configuration

import java.io.File

import com.google.inject.ImplementedBy
import com.typesafe.config.{Config, ConfigFactory}
import net.degols.libs.cluster.ClusterTools
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

@ImplementedBy(classOf[DefaultClusterConfiguration])
trait ClusterConfigurationApi {
  /**
    * General execution context to use in the system
    */
  implicit val executionContext: ExecutionContext

  val localHostname: Future[String]

  /**
    * A watcher might not receive any message back from an ElectionActor directly. Or we want to allow a nice switch
    * of ElectionManagers without killing all Workers directly. During that time, we can have duplicate work, so the value
    * should be correctly chosen. 1 minute should be more than enough.
    */
  val watcherTimeoutBeforeSuicide: Future[FiniteDuration]

  /**
    * A Soft Distribution message is sent frequently to start missing actors.
    */
  val softWorkDistributionFrequency: Future[FiniteDuration]

  /**
    * A Hard Distribution message is sent from time to time to stop existing actors, and start them elsewhere.
    */
  val hardWorkDistributionFrequency: Future[FiniteDuration]

  /**
    * How much time do we allow to start a WorkerOrder before considering as failing?
    */
  val startWorkerTimeout: Future[FiniteDuration]

  /**
    * How many entries of ClusterInfo can we keep in the cache?
    * Normally there is no need to keep track of 1000s of them
    */
  val clusterInfoCacheSize: Future[Int]

  /**
    * How much time (seconds) should we keep a ClusterInfo entry
    */
  val clusterInfoCacheTimeout: Future[Int]

  /**
    * Maximum timeout allowed to ask for info to the manager
    */
  val clusterInfoTimeout: Future[Int]

  /**
    * It's difficult to get a remote actor path locally. Because of that, we still want to know the current hostname + port
    */
  val akkaLocalHostname: Future[String]
  val akkaLocalPort: Future[Int]
}

/**
  * The ClusterConfigurationApi is only loaded once at startup, then we create a ClusterConfiguration that we pass from
  * one class to another, to avoid having futures everywhere just for the fun.
  *
  * This class should not be exposed outside of the library as the developer might want to directly inject it, thinking
  * it's the same as ClusterConfigurationApi. To avoid any misunderstanding, we make it private.
  */
private[cluster] case class ClusterConfiguration(
                                 executionContext: ExecutionContext,
                                 localHostname: String,
                                 watcherTimeoutBeforeSuicide: FiniteDuration,
                                 softWorkDistributionFrequency: FiniteDuration,
                                 hardWorkDistributionFrequency: FiniteDuration,
                                 startWorkerTimeout: FiniteDuration,
                                 clusterInfoCacheSize: Int,
                                 clusterInfoCacheTimeout: Int,
                                 clusterInfoTimeout: Int,
                                 akkaLocalHostname: String,
                                 akkaLocalPort: Int
                               )

object ClusterConfiguration {
  def load(clusterConfigurationApi: ClusterConfigurationApi)(implicit ec: ExecutionContext): Future[ClusterConfiguration] = {
    for {
      localHostname <- clusterConfigurationApi.localHostname
      watcherTimeoutBeforeSuicide <- clusterConfigurationApi.watcherTimeoutBeforeSuicide
      softWorkDistributionFrequency <- clusterConfigurationApi.softWorkDistributionFrequency
      hardWorkDistributionFrequency <- clusterConfigurationApi.hardWorkDistributionFrequency
      startWorkerTimeout <- clusterConfigurationApi.startWorkerTimeout
      clusterInfoCacheSize <- clusterConfigurationApi.clusterInfoCacheSize
      clusterInfoCacheTimeout <- clusterConfigurationApi.clusterInfoCacheTimeout
      clusterInfoTimeout <- clusterConfigurationApi.clusterInfoTimeout
      akkaLocalHostname <- clusterConfigurationApi.akkaLocalHostname
      akkaLocalPort <- clusterConfigurationApi.akkaLocalPort
    } yield {
      ClusterConfiguration(
        clusterConfigurationApi.executionContext,
        localHostname,
        watcherTimeoutBeforeSuicide,
        softWorkDistributionFrequency,
        hardWorkDistributionFrequency,
        startWorkerTimeout,
        clusterInfoCacheSize,
        clusterInfoCacheTimeout,
        clusterInfoTimeout,
        akkaLocalHostname,
        akkaLocalPort
      )
    }
  }
}