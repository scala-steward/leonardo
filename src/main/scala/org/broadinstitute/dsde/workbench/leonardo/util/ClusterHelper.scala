package org.broadinstitute.dsde.workbench.leonardo.util

import java.io.File
import java.nio.charset.Charset

import akka.actor.ActorSystem
import cats.data.OptionT
import cats.effect._
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleProjectDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject, ServiceAccountKey, ServiceAccountKeyId, generateUniqueBucketName}
import cats.implicits._
import cats._
import cats.data._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.{ExecutionContext, Future}
import fs2._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.leonardo.Metrics
import org.broadinstitute.dsde.workbench.leonardo.util.TemplateHelper.GcsResource
import scala.collection.JavaConverters._

class ClusterHelper(dataprocConfig: DataprocConfig,
                    proxyConfig: ProxyConfig,
                    gdDAO: GoogleDataprocDAO,
                    googleComputeDAO: GoogleComputeDAO,
                    googleIamDAO: GoogleIamDAO,
                    googleProjectDAO: GoogleProjectDAO,
                    clusterFilesConfig: ClusterFilesConfig,
                    clusterResourcesConfig: ClusterResourcesConfig,
                    clusterDefaultsConfig: ClusterDefaultsConfig,
                    bucketHelper: BucketHelper)
                   (implicit val executionContext: ExecutionContext, val system: ActorSystem) extends LazyLogging with Retry {

  implicit val cs = IO.contextShift(executionContext)

  private def firewallRule = FirewallRule(
    name = FirewallRuleName(dataprocConfig.firewallRuleName),
    protocol = FirewallRuleProtocol(proxyConfig.jupyterProtocol),
    ports = List(FirewallRulePort(proxyConfig.jupyterPort.toString)),
    network = dataprocConfig.vpcNetwork.map(VPCNetworkName),
    targetTags = List(NetworkTag(dataprocConfig.networkTag))
  )

  private def whenGoogle409(throwable: Throwable): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => t.getStatusCode == 409
      case _ => false
    }
  }

  private def whenGoogleZoneCapacityIssue(throwable: Throwable): Boolean = {
    throwable match {
      case t: GoogleJsonResponseException => t.getStatusCode == 429 && t.getDetails.getErrors.asScala.head.getReason.equalsIgnoreCase("rateLimitExceeded")
      case _ => false
    }
  }

  private[service] def generateServiceAccountKey(googleProject: GoogleProject, serviceAccountEmailOpt: Option[WorkbenchEmail]): Future[Option[ServiceAccountKey]] = {
    // TODO: implement google2 version of GoogleIamDAO
    serviceAccountEmailOpt.traverse { email =>
      googleIamDAO.createServiceAccountKey(googleProject, email)
    }
  }

  // TODO LeoService.delete needs to call this
  private def removeServiceAccountKey(googleProject: GoogleProject, serviceAccountEmailOpt: Option[WorkbenchEmail], serviceAccountKeyIdOpt: Option[ServiceAccountKeyId]): Future[Unit] = {
    // TODO: implement google2 version of GoogleIamDAO
    (serviceAccountEmailOpt, serviceAccountKeyIdOpt).mapN { case (email, key) =>
      googleIamDAO.removeServiceAccountKey(googleProject, email, key)
    } getOrElse Future.unit
  }


  def addDataprocWorkerRoleToServiceAccount(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail]): Future[Unit] = {
    serviceAccountOpt.map { serviceAccountEmail =>
      // TODO: implement google2 version of GoogleIamDAO
      // Retry 409s with exponential backoff. This can happen if concurrent policy updates are made in the same project.
      // Google recommends a retry in this case.
      val iamResult: Future[Unit] = retryExponentially(whenGoogle409, s"IAM policy change failed for Google project '$googleProject'") { () =>
        googleIamDAO.addIamRolesForUser(googleProject, serviceAccountEmail, Set("roles/dataproc.worker"))
      }
      iamResult
    } getOrElse Future.unit
  }


  def removeDataprocWorkerRoleFromServiceAccount(googleProject: GoogleProject, serviceAccountOpt: Option[WorkbenchEmail]): Future[Unit] = {
    serviceAccountOpt.map { serviceAccountEmail =>
      // TODO: implement google2 version of GoogleIamDAO
      // Retry 409s with exponential backoff. This can happen if concurrent policy updates are made in the same project.
      // Google recommends a retry in this case.
      val iamResult: Future[Unit] = retryExponentially(whenGoogle409, s"IAM policy change failed for Google project '$googleProject'") { () =>
        googleIamDAO.removeIamRolesForUser(googleProject, serviceAccountEmail, Set("roles/dataproc.worker"))
      }
      iamResult
    } getOrElse Future.unit
  }

  /* Process the templated cluster init script and put all initialization files in the init bucket */
  private[service] def initializeBucketObjects(cluster: Cluster, initBucketName: GcsBucketName, serviceAccountKey: Option[ServiceAccountKey]): Stream[IO, Unit] = {

    // Build a mapping of (name, value) pairs with which to apply templating logic to resources
    val replacements = ClusterInitValues(cluster, initBucketName, serviceAccountKey, dataprocConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig).toMap

    // Raw files to upload to the bucket, no additional processing needed.
    val rawFilesToUpload = Stream.emits[IO, File](
      Seq(
        clusterFilesConfig.jupyterServerCrt,
        clusterFilesConfig.jupyterServerKey,
        clusterFilesConfig.jupyterRootCaPem
      )
    ).evalMap[IO, GcsResource] { f =>
      TemplateHelper.readFile[IO](f, executionContext)
    }

    val rawResourcesToUpload = Stream.emits(
      Seq(
        clusterResourcesConfig.jupyterDockerCompose,
        clusterResourcesConfig.rstudioDockerCompose,
        clusterResourcesConfig.proxyDockerCompose,
        clusterResourcesConfig.proxySiteConf,
        clusterResourcesConfig.extensionEntry,
        clusterResourcesConfig.jupyterLabGooglePlugin,
        clusterResourcesConfig.welderDockerCompose
      )
    ).evalMap[IO, GcsResource] { resource =>
      TemplateHelper.readResource(resource, executionContext)
    }

    val templatedResourcesToUpload = Stream.emits(
      Seq(
        clusterResourcesConfig.initActionsScript,
        clusterResourcesConfig.googleSignInJs,
        clusterResourcesConfig.editModeJs,
        clusterResourcesConfig.safeModeJs,
        clusterResourcesConfig.jupyterNotebookConfigUri
      )
    ).evalMap[IO, GcsResource] { resource =>
      TemplateHelper.templateResource(replacements, resource, executionContext)
    }

    val privateKey = Stream.emit(
      for {
        k <- serviceAccountKey
        d <- k.privateKeyData.decode
      } yield GcsResource(GcsBlobName(ClusterInitValues.serviceAccountCredentialsFilename), d.getBytes(Charset.defaultCharset))
    ).unNone

    (rawFilesToUpload ++ rawResourcesToUpload ++ templatedResourcesToUpload ++ privateKey).evalMap[IO, Unit] { resource =>
      bucketHelper.storeObject(initBucketName, resource.gcsBlobName, resource.content, "text/plain")
    }
  }


  def createGoogleCluster(cluster: Cluster): IO[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = {
    val initBucketName = generateUniqueBucketName("leoinit-"+cluster.clusterName.value)
    val stagingBucketName = generateUniqueBucketName("leostaging-"+cluster.clusterName.value)

    // memoized
    val serviceAccountKeyFuture = generateServiceAccountKey(cluster.googleProject, cluster.serviceAccountInfo.notebookServiceAccount)
    val serviceAccountKeyIO = IO.fromFuture(IO(serviceAccountKeyFuture))

    val ioResult: IO[(Cluster, GcsBucketName, Option[ServiceAccountKey])] = for {
      // Create the firewall rule in the google project if it doesn't already exist, so we can access the cluster
      _ <- IO.fromFuture(IO(googleComputeDAO.updateFirewallRule(cluster.googleProject, firewallRule)))

      // Generate a service account key for the notebook service account (if present) to localize on the cluster.
      // We don't need to do this for the cluster service account because its credentials are already
      // on the metadata server.
      serviceAccountKeyOpt <- serviceAccountKeyIO

      // Add Dataproc Worker role to the cluster service account, if present.
      // This is needed to be able to spin up Dataproc clusters.
      // If the Google Compute default service account is being used, this is not necessary.
      _ <- IO.fromFuture(IO(addDataprocWorkerRoleToServiceAccount(cluster.googleProject, cluster.serviceAccountInfo.clusterServiceAccount)))

      // Create the bucket in the cluster's google project and populate with initialization files.
      // ACLs are granted so the cluster service account can access the files at initialization time.
      _ <- bucketHelper.createInitBucket(cluster.googleProject, initBucketName, cluster.serviceAccountInfo).compile.drain
      _ <- initializeBucketObjects(cluster, initBucketName, serviceAccountKeyOpt).compile.drain

      // Create the cluster staging bucket. ACLs are granted so the user/pet can access it.
      _ <- bucketHelper.createStagingBucket(cluster.auditInfo.creator, cluster.googleProject, stagingBucketName, cluster.serviceAccountInfo)

      // build cluster configuration
      machineConfig = cluster.machineConfig
      initScript = GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.initActionsScript.value))
   //   autopauseThreshold = cluster.autopauseThreshold
    //  clusterScopes = if(clusterRequest.scopes.isEmpty) dataprocConfig.defaultScopes else clusterRequest.scopes
      credentialsFileName = cluster.serviceAccountInfo.notebookServiceAccount.map(_ => s"/etc/${ClusterInitValues.serviceAccountCredentialsFilename}")

      // decide whether to use VPC network
      lookupProjectLabels = dataprocConfig.projectVPCNetworkLabel.isDefined || dataprocConfig.projectVPCSubnetLabel.isDefined
      projectLabels <- if (lookupProjectLabels) IO.fromFuture(IO(googleProjectDAO.getLabels(cluster.googleProject.value))) else IO(Map.empty[String, String])
      clusterVPCSettings = getClusterVPCSettings(projectLabels)

      // Create the cluster
      createClusterConfig = CreateClusterConfig(machineConfig, initScript, cluster.serviceAccountInfo.clusterServiceAccount, credentialsFileName, stagingBucketName, cluster.scopes, clusterVPCSettings, cluster.properties)
      retryResult <- IO.fromFuture(IO(retryExponentially(whenGoogleZoneCapacityIssue, "Cluster creation failed because zone with adequate resources was not found") { () =>
        gdDAO.createCluster(cluster.googleProject, cluster.clusterName, createClusterConfig)
      }))


      operation <- retryResult match {
        case Right((errors, op)) if errors == List.empty => IO(op)
        case Right((errors, op)) =>
          Metrics.newRelic.incrementCounterIO("zoneCapacityClusterCreationFailure", errors.length)
            .as(op)
        case Left(errors) =>
          Metrics.newRelic.incrementCounterIO("zoneCapacityClusterCreationFailure", errors.filter(whenGoogleZoneCapacityIssue).length)
            .flatMap(_ => IO.raiseError(errors.head))
      }

      cluster = Cluster.createFinal(cluster, operation, stagingBucketName)
    } yield (cluster, initBucketName, serviceAccountKeyOpt)


    ioResult.handleErrorWith { throwable =>
      cleanUpGoogleResourcesOnError(cluster.googleProject,
        cluster.clusterName, initBucketName,
        cluster.serviceAccountInfo, serviceAccountKeyIO) >> IO.raiseError(throwable)
    }

  }

  def getClusterVPCSettings(projectLabels: Map[String, String]): Option[Either[VPCNetworkName, VPCSubnetName]] = {
    //Dataproc only allows you to specify a subnet OR a network. Subnets will be preferred if present.
    //High-security networks specified inside of the project will always take precedence over anything
    //else. Thus, VPC configuration takes the following precedence:
    // 1) High-security subnet in the project (if present)
    // 2) High-security network in the project (if present)
    // 3) Subnet specified in leonardo.conf (if present)
    // 4) Network specified in leonardo.conf (if present)
    // 5) The default network in the project
    val projectSubnet  = dataprocConfig.projectVPCSubnetLabel.flatMap(subnetLabel => projectLabels.get(subnetLabel).map(VPCSubnetName) )
    val projectNetwork = dataprocConfig.projectVPCNetworkLabel.flatMap( networkLabel => projectLabels.get(networkLabel).map(VPCNetworkName) )
    val configSubnet   = dataprocConfig.vpcSubnet.map(VPCSubnetName)
    val configNetwork  = dataprocConfig.vpcNetwork.map(VPCNetworkName)

    (projectSubnet, projectNetwork, configSubnet, configNetwork) match {
      case (Some(subnet), _, _, _)  => Some(Right(subnet))
      case (_, Some(network), _, _) => Some(Left(network))
      case (_, _, Some(subnet), _)  => Some(Right(subnet))
      case (_, _, _, Some(network)) => Some(Left(network))
      case (_, _, _, _)             => None
    }
  }

  private[service] def cleanUpGoogleResourcesOnError(googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo, serviceAccountKey: IO[Option[ServiceAccountKey]]): IO[Unit] = {
    logger.error(s"Cluster creation failed in Google for $googleProject / ${clusterName.value}. Cleaning up resources in Google...")

    // Clean up resources in Google
    val deleteBucket = bucketHelper.deleteInitBucket(initBucketName) map { _ =>
      logger.info(s"Successfully deleted init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}")
    } recover { case e =>
      logger.error(s"Failed to delete init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}", e)
    }

    // Don't delete the staging bucket so the user can see error logs.

    val deleteCluster = IO.fromFuture(IO(gdDAO.deleteCluster(googleProject, clusterName))) map { _ =>
      logger.info(s"Successfully deleted cluster ${googleProject.value} / ${clusterName.value}")
    } recover { case e =>
      logger.error(s"Failed to delete cluster ${googleProject.value} / ${clusterName.value}", e)
    }

    val deleteServiceAccountKey = (for {
      key <- serviceAccountKey
      _ <- IO.fromFuture(IO(removeServiceAccountKey(googleProject, serviceAccountInfo.notebookServiceAccount, key.map(_.id)))
    } yield {
      logger.info(s"Successfully deleted service account key for ${serviceAccountInfo.notebookServiceAccount}")
    }) recover { case e =>
      logger.error(s"Failed to delete service account key for ${serviceAccountInfo.notebookServiceAccount}", e)
    }

    // TODO also remove dataproc role

    // TODO: parJoin?
    (Stream.eval(deleteBucket) ++ Stream.eval(deleteCluster) ++ Stream.eval(deleteServiceAccountKey)).compile.drain
  }
}