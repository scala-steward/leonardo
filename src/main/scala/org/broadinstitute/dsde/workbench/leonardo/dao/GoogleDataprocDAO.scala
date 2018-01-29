package org.broadinstitute.dsde.workbench.leonardo.dao

import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.config.ClusterDefaultsConfig
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsPath, GoogleProject}

import scala.concurrent.{ExecutionContext, Future}

trait GoogleDataprocDAO {
  def createCluster(googleProject: GoogleProject,
                    clusterName: ClusterName,
                    machineConfig: MachineConfig,
                    initScript: GcsPath,
                    serviceAccountInfo: ServiceAccountInfo): Future[Operation]

  def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit]

  def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus]

  def listClusters(googleProject: GoogleProject): Future[List[UUID]]

  def getClusterMasterInstanceIp(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[IP]]

  def getClusterErrorDetails(operationName: OperationName): Future[Option[ClusterErrorDetails]]

  def updateFirewallRule(googleProject: GoogleProject, firewallRule: FirewallRule): Future[Unit]

  def getUserInfoAndExpirationFromAccessToken(accessToken: String): Future[(UserInfo, Instant)]

  def getComputeEngineDefaultServiceAccount(googleProject: GoogleProject): Future[Option[WorkbenchEmail]]
}
