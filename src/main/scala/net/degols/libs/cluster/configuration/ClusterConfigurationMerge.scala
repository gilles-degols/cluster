package net.degols.libs.cluster.configuration
import javax.inject.Singleton
import net.degols.libs.election.ConfigurationMerge

@Singleton
class ClusterConfigurationMerge extends ConfigurationMerge {
  override val filenames: Seq[String] = Seq("application.cluster.conf","application.election.conf")
}
