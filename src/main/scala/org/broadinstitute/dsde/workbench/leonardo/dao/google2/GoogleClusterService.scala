package org.broadinstitute.dsde.workbench.leonardo.dao.google2

import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import scala.language.higherKinds

trait GoogleClusterService[F[_]] {

  type ClusterConfiguration
  type ClusterOperation

  def preprocessCluster(cluster: Cluster): F[ClusterConfiguration]

  def createCluster(cluster: Cluster, clusterConfiguration: ClusterConfiguration): F[ClusterOperation]

}
