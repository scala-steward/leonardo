package org.broadinstitute.dsde.workbench.leonardo.dao.google2

import cats.effect.{Effect, IO}
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster
import com.google.api.services.container.Container
import org.broadinstitute.dsde.workbench.leonardo.dao.google2.KubernetesInterpreter.KubernetesConfiguration
import org.broadinstitute.dsde.workbench.leonardo.model.google.Operation

import scala.concurrent.ExecutionContext

class KubernetesInterpreter private[google2](db: Container) extends GoogleClusterService[IO] {
  override type ClusterConfiguration = KubernetesConfiguration
  override type ClusterOperation = Operation

  override def preprocessCluster(cluster: Cluster): IO[ClusterConfiguration] = ???

  override def createCluster(cluster: Cluster, clusterConfiguration: ClusterConfiguration): IO[Operation] = {
    db.projects().zones().clusters().create(s,dfgmlsfg)
  }
}

object KubernetesInterpreter {

  case class KubernetesConfiguration(foo: Int)

}