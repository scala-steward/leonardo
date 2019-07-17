package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.Metrics
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus, CreateClusterConfig, Instance}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor.{ClusterMonitorMessage, ScheduleMonitorPass}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject, ServiceAccountKey, generateUniqueBucketName}

import scala.collection.immutable.Set
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object ClusterMonitorUtils {

  private def handleProvisioningCluster(cluster: Cluster): IO[Unit] = {

  }

  private[service] def createGoogleCluster(
                                           cluster: Cluster)
                                          (implicit executionContext: ExecutionContext): Future[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    createGoogleCluster(userEmail, cluster.serviceAccountInfo, cluster.googleProject, cluster.clusterName, clusterRequest, cluster.clusterImages)
  }

  /* Creates a cluster in the given google project:
     - Add a firewall rule to the user's google project if it doesn't exist, so we can access the cluster
     - Create the initialization bucket for the cluster in the leo google project
     - Upload all the necessary initialization files to the bucket
     - Create the cluster in the google project */
  private[service] def createGoogleCluster(userEmail: WorkbenchEmail,
                                           serviceAccountInfo: ServiceAccountInfo,
                                           googleProject: GoogleProject,
                                           clusterName: ClusterName,
                                           clusterRequest: ClusterRequest,
                                           clusterImages: Set[ClusterImage])
                                          (implicit executionContext: ExecutionContext): Future[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    val initBucketName = generateUniqueBucketName("leoinit-"+clusterName.value)
    val stagingBucketName = generateUniqueBucketName("leostaging-"+clusterName.value)

    val googleFuture = for {
      // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
      _ <- googleComputeDAO.updateFirewallRule(googleProject, firewallRule)

      // Generate a service account key for the notebook service account (if present) to localize on the cluster.
      // We don't need to do this for the cluster service account because its credentials are already
      // on the metadata server.
      serviceAccountKeyOpt <- generateServiceAccountKey(googleProject, serviceAccountInfo.notebookServiceAccount)

      // Add Dataproc Worker role to the cluster service account, if present.
      // This is needed to be able to spin up Dataproc clusters.
      // If the Google Compute default service account is being used, this is not necessary.
      _ <- addDataprocWorkerRoleToServiceAccount(googleProject, serviceAccountInfo.clusterServiceAccount)

      // Create the bucket in the cluster's google project and populate with initialization files.
      // ACLs are granted so the cluster service account can access the files at initialization time.
      initBucket <- bucketHelper.createInitBucket(googleProject, initBucketName, serviceAccountInfo)
      _ <- initializeBucketObjects(userEmail, googleProject, clusterName, initBucket, clusterRequest, serviceAccountKeyOpt, contentSecurityPolicy, clusterImages, stagingBucketName)

      // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
      stagingBucket <- bucketHelper.createStagingBucket(userEmail, googleProject, stagingBucketName, serviceAccountInfo)

      // build cluster configuration
      machineConfig = MachineConfigOps.create(clusterRequest.machineConfig, clusterDefaultsConfig)
      initScript = GcsPath(initBucket, GcsObjectName(clusterResourcesConfig.initActionsScript.value))
      autopauseThreshold = calculateAutopauseThreshold(clusterRequest.autopause, clusterRequest.autopauseThreshold)
      clusterScopes = if(clusterRequest.scopes.isEmpty) dataprocConfig.defaultScopes else clusterRequest.scopes
      credentialsFileName = serviceAccountInfo.notebookServiceAccount.map(_ => s"/etc/${ClusterInitValues.serviceAccountCredentialsFilename}")

      // decide whether to use VPC network
      lookupProjectLabels = dataprocConfig.projectVPCNetworkLabel.isDefined || dataprocConfig.projectVPCSubnetLabel.isDefined
      projectLabels <- if (lookupProjectLabels) googleProjectDAO.getLabels(googleProject.value) else Future.successful(Map.empty[String, String])
      clusterVPCSettings = getClusterVPCSettings(projectLabels)

      // Create the cluster
      createClusterConfig = CreateClusterConfig(machineConfig, initScript, serviceAccountInfo.clusterServiceAccount, credentialsFileName, stagingBucket, clusterScopes, clusterVPCSettings, clusterRequest.properties)
      retryResult <- retryExponentially(whenGoogleZoneCapacityIssue, "Cluster creation failed because zone with adequate resources was not found") { () =>
        gdDAO.createCluster(googleProject, clusterName, createClusterConfig)
      }
      operation <- retryResult match {
        case Right((errors, op)) if errors == List.empty => Future.successful(op)
        case Right((errors, op)) =>
          Metrics.newRelic.incrementCounterIO("zoneCapacityClusterCreationFailure", errors.length).unsafeRunAsync(_ => ())
          Future.successful(op)
        case Left(errors) =>
          Metrics.newRelic.incrementCounterIO("zoneCapacityClusterCreationFailure", errors.filter(whenGoogleZoneCapacityIssue).length).unsafeRunAsync(_ => ())
          Future.failed(errors.head)
      }
      cluster = Cluster.create(clusterRequest, userEmail, clusterName, googleProject, serviceAccountInfo,
        machineConfig, dataprocConfig.clusterUrlBase, autopauseThreshold, clusterScopes, Some(operation), Option(stagingBucket), clusterImages)
    } yield (cluster, initBucket, serviceAccountKeyOpt)

    // If anything fails, we need to clean up Google resources that might have been created
    googleFuture.andThen { case Failure(t) =>
      // Don't wait for this future
      cleanUpGoogleResourcesOnError(t, googleProject, clusterName, initBucketName, serviceAccountInfo)
    }
  }

}
