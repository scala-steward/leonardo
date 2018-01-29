package org.broadinstitute.dsde.workbench.leonardo.dao

import java.io.File
import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.data.OptionT
import cats.instances.future._
import cats.syntax.functor._
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.BigqueryScopes
import com.google.api.services.cloudresourcemanager.{CloudResourceManager, CloudResourceManagerScopes}
import com.google.api.services.compute.model.Firewall.Allowed
import com.google.api.services.compute.model.{Firewall, Instance}
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.dataproc.Dataproc
import com.google.api.services.dataproc.model.{NodeInitializationAction, Cluster => DataprocCluster, Operation => DataprocOperation, _}
import com.google.api.services.oauth2.Oauth2.Builder
import com.google.api.services.oauth2.Oauth2Scopes
import org.broadinstitute.dsde.workbench.google.GoogleUtilities
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.{ClusterStatus => LeoClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterErrorDetails, ClusterInitValues, ClusterName, FirewallRule, IP, InstanceName, LeoException, MachineConfig, Operation, OperationName, ServiceAccountInfo, ZoneUri, Cluster => LeoCluster, ClusterStatus => LeoClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.service.AuthorizationError
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}

case class CallToGoogleApiFailedException(googleProject: GoogleProject, context: String, exceptionStatusCode: Int, errorMessage: String)
  extends LeoException(s"Call to Google API failed for ${googleProject.value} / $context. Message: $errorMessage", exceptionStatusCode)

class GoogleDataprocDAO(leoServiceAccountEmail: WorkbenchEmail,
                        leoServiceAccountPemFile: File,
                        appName: String,
                        defaultRegion: String,
                        defaultNetworkTag: String,
                        override val workbenchMetricBaseName: String)
                       (implicit val system: ActorSystem, val executionContext: ExecutionContext)
  extends DataprocDAO with GoogleUtilities {

  implicit val service = GoogleInstrumentedService.Dataproc

  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val jsonFactory = JacksonFactory.getDefaultInstance

  private lazy val cloudPlatformScopes = List(ComputeScopes.CLOUD_PLATFORM)
  private lazy val vmScopes = List(ComputeScopes.COMPUTE, ComputeScopes.CLOUD_PLATFORM)
  private lazy val cloudResourceManagerScopes = List(CloudResourceManagerScopes.CLOUD_PLATFORM)
  private lazy val oauth2Scopes = List(Oauth2Scopes.USERINFO_EMAIL, Oauth2Scopes.USERINFO_PROFILE)
  private lazy val bigqueryScopes = List(BigqueryScopes.BIGQUERY)

  private lazy val oauth2 =
    new Builder(httpTransport, jsonFactory, null)
      .setApplicationName(appName).build()

  private lazy val dataproc = {
    new Dataproc.Builder(httpTransport, jsonFactory, getServiceAccountCredential(cloudPlatformScopes))
      .setApplicationName(appName).build()
  }

  private lazy val compute = {
    new Compute.Builder(httpTransport, jsonFactory, getServiceAccountCredential(vmScopes))
      .setApplicationName(appName).build()
  }

  private lazy val cloudResourceManager = {
    new CloudResourceManager.Builder(httpTransport, jsonFactory, getServiceAccountCredential(cloudResourceManagerScopes))
      .setApplicationName(appName).build()
  }

  private def getServiceAccountCredential(scopes: List[String]): Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(leoServiceAccountEmail.value)
      .setServiceAccountScopes(scopes.asJava)
      .setServiceAccountPrivateKeyFromPemFile(leoServiceAccountPemFile)
      .build()
  }

  /* Kicks off building the cluster. This will return before the cluster finishes creating. */
  override def createCluster(googleProject: GoogleProject, clusterName: ClusterName, machineConfig: MachineConfig, initScript: GcsPath, serviceAccountInfo: ServiceAccountInfo): Future[Operation] = {
    val cluster = new DataprocCluster()
      .setClusterName(clusterName.value)
      .setConfig(getClusterConfig(machineConfig, initScript, serviceAccountInfo))

    val request = dataproc.projects().regions().clusters().create(googleProject.value, defaultRegion, cluster)

    executeGoogleRequestAsync(googleProject, clusterName.toString, request).map { op =>
      Operation(OperationName(op.getName), getOperationUUID(op))
    }
  }

  /* Delete a cluster within the google project */
  override def deleteCluster(googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    val request = dataproc.projects().regions().clusters().delete(googleProject.value, defaultRegion, clusterName.value)
    executeGoogleRequestAsync(googleProject, clusterName.toString, request).recover {
      // treat a 404 error as a successful deletion
      case CallToGoogleApiFailedException(_, _, 404, _) => ()
      case CallToGoogleApiFailedException(_, _, 400, msg) if msg.contains("it has other pending delete operations against it") => ()
    }.void
  }

  override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[LeoClusterStatus] = {
    getCluster(googleProject, clusterName).map { cluster =>
      LeoClusterStatus.withNameIgnoreCase(cluster.getStatus.getState)
    }
  }

  override def listClusters(googleProject: GoogleProject): Future[List[UUID]] = {
    val request = dataproc.projects().regions().clusters().list(googleProject.value, defaultRegion)
    // Use OptionT to handle nulls in the Google response
    val transformed = for {
      result <- OptionT.liftF(executeGoogleRequestAsync(googleProject, "", request))
      googleClusters <- OptionT.pure(result.getClusters)
    } yield {
      googleClusters.asScala.toList.map(c => UUID.fromString(c.getClusterUuid))
    }
    transformed.value.map(_.getOrElse(List.empty))
  }

  override def getClusterMasterInstanceIp(googleProject: GoogleProject, clusterName: ClusterName): Future[Option[IP]] = {
    // OptionT is handy when you potentially want to deal with Future[A], Option[A],
    // Future[Option[A]], and A all in the same flatMap!
    //
    // Legend:
    // - OptionT.pure turns an A into an OptionT[F, A]
    // - OptionT.liftF turns an F[A] into an OptionT[F, A]
    // - OptionT.fromOption turns an Option[A] into an OptionT[F, A]

    val ipOpt: OptionT[Future, IP] = for {
      cluster <- OptionT.liftF[Future, DataprocCluster] { getCluster(googleProject, clusterName) }
      masterInstanceName <- OptionT.fromOption { getMasterInstanceName(cluster) }
      masterInstanceZone <- OptionT.fromOption { getZone(cluster) }
      masterInstance <- OptionT.liftF[Future, Instance] { getInstance(googleProject, masterInstanceZone, masterInstanceName) }
      masterInstanceIp <- OptionT.fromOption { getInstanceIP(masterInstance) }
    } yield masterInstanceIp

    // OptionT[Future, String] is simply a case class wrapper for Future[Option[String]].
    // So this just grabs the inner value and returns it.
    ipOpt.value
  }

  override def getClusterErrorDetails(operationName: OperationName): Future[Option[ClusterErrorDetails]] = {
    val errorOpt: OptionT[Future, ClusterErrorDetails] = for {
      operation <- OptionT.liftF[Future, DataprocOperation] { getOperation(operationName) } if operation.getDone
      error <- OptionT.pure { operation.getError }
      code <- OptionT.pure { error.getCode }
    } yield ClusterErrorDetails(code, Option(error.getMessage))

    errorOpt.value
  }

  /* Check if the given google project has a cluster firewall rule. If not, add the rule to the project*/
  override def updateFirewallRule(googleProject: GoogleProject, firewallRule: FirewallRule): Future[Unit] = {
    val request = compute.firewalls().get(googleProject.value, firewallRule.name.value)
    executeGoogleRequestAsync(googleProject, firewallRule.name.toString, request).recoverWith {
      case CallToGoogleApiFailedException(_, _, 404, _) =>
        addFirewallRule(googleProject, firewallRule)
    }.void
  }

  override def getComputeEngineDefaultServiceAccount(googleProject: GoogleProject): Future[Option[WorkbenchEmail]] = {
    getProjectNumber(googleProject).map { numberOpt =>
      numberOpt.map { number =>
        // Service account email format documented in:
        // https://cloud.google.com/compute/docs/access/service-accounts#compute_engine_default_service_account
        WorkbenchEmail(s"$number-compute@developer.gserviceaccount.com")
      }
    }
  }

  /* Adds a firewall rule in the given google project. This firewall rule allows ingress traffic through a specified port for all
     VMs with the network tag "leonardo". This rule should only be added once per project.
    To think about: do we want to remove this rule if a google project no longer has any clusters? */
  private def addFirewallRule(googleProject: GoogleProject, firewallRule: FirewallRule): Future[Unit] = {
    val allowed = new Allowed().setIPProtocol(firewallRule.protocol).setPorts(firewallRule.ports.map(_.value).asJava)
    val googleFirewall = new Firewall()
      .setName(firewallRule.name.value)
      .setTargetTags(firewallRule.targetTags.map(_.value).asJava)
      .setAllowed(List(allowed).asJava)
      .setNetwork(firewallRule.network.value)

    val request = compute.firewalls().insert(googleProject.value, googleFirewall)
    executeGoogleRequestAsync(googleProject, firewallRule.name.value, request).void
  }

  private def getClusterConfig(machineConfig: MachineConfig, initScript: GcsPath, serviceAccountInfo: ServiceAccountInfo): ClusterConfig = {
    // Create a GceClusterConfig, which has the common config settings for resources of Google Compute Engine cluster instances,
    // applicable to all instances in the cluster.
    // Set the network tag, which is needed by the firewall rule that allows leo to talk to the cluster
    val gceClusterConfig = new GceClusterConfig().setTags(List(defaultNetworkTag).asJava)

    // Set the cluster service account, if present.
    // This is the service account passed to the create cluster API call.
    serviceAccountInfo.clusterServiceAccount.foreach { serviceAccountEmail =>
      gceClusterConfig.setServiceAccount(serviceAccountEmail.value).setServiceAccountScopes((oauth2Scopes ++ bigqueryScopes).asJava)
    }

    // Create a NodeInitializationAction, which specifies the executable to run on a node.
    // This executable is our init-actions.sh, which will stand up our jupyter server and proxy.
    val initActions = Seq(new NodeInitializationAction().setExecutableFile(initScript.toUri))

    // Create a config for the master node, if properties are not specified in request, use defaults
    val masterConfig = new InstanceGroupConfig()
      .setMachineTypeUri(machineConfig.masterMachineType.get)
      .setDiskConfig(new DiskConfig().setBootDiskSizeGb(machineConfig.masterDiskSize.get))

    // Create a Cluster Config and give it the GceClusterConfig, the NodeInitializationAction and the InstanceGroupConfig
    createClusterConfig(machineConfig, serviceAccountInfo)
      .setGceClusterConfig(gceClusterConfig)
      .setInitializationActions(initActions.asJava)
      .setMasterConfig(masterConfig)
  }

  // Expects a Machine Config with master configs defined for a 0 worker cluster and both master and worker
  // configs defined for 2 or more workers.
  private def createClusterConfig(machineConfig: MachineConfig, serviceAccountInfo: ServiceAccountInfo): ClusterConfig = {

    val swConfig: SoftwareConfig = getSoftwareConfig(machineConfig.numberOfWorkers, serviceAccountInfo)

    // If the number of workers is zero, make a Single Node cluster, else make a Standard one
    if (machineConfig.numberOfWorkers.get == 0) {
      new ClusterConfig().setSoftwareConfig(swConfig)
    }
    else // Standard, multi node cluster
      getMultiNodeClusterConfig(machineConfig).setSoftwareConfig(swConfig)
  }

  private def getSoftwareConfig(numWorkers: Option[Int], serviceAccountInfo: ServiceAccountInfo) = {
    val authProps: Map[String, String] = serviceAccountInfo.notebookServiceAccount match {
      case None =>
        // If we're not using a notebook service account, no need to set Hadoop properties since
        // the SA credentials are on the metadata server.
        Map.empty

      case Some(_) =>
        // If we are using a notebook service account, set the necessary Hadoop properties
        // to specify the location of the notebook service account key file.
        Map(
          "core:google.cloud.auth.service.account.enable" -> "true",
          "core:google.cloud.auth.service.account.json.keyfile" -> s"/etc/${ClusterInitValues.serviceAccountCredentialsFilename}"
        )
    }

    val dataprocProps: Map[String, String] = if (numWorkers.get == 0) {
      // Set a SoftwareConfig property that makes the cluster have only one node
      Map("dataproc:dataproc.allow.zero.workers" -> "true")
    }
    else
      Map.empty

    val yarnProps = Map(
      // Helps with debugging
      "yarn:yarn.log-aggregation-enable" -> "true",

      // Dataproc 1.1 sets this too high (5586m) which limits the number of Spark jobs that can be run at one time.
      // This has been reduced drastically in Dataproc 1.2. See:
      // https://stackoverflow.com/questions/41185599/spark-default-settings-on-dataproc-especially-spark-yarn-am-memory
      // GAWB-3080 is open for upgrading to Dataproc 1.2, at which point this line can be removed.
      "spark:spark.yarn.am.memory" -> "640m"
    )

    new SoftwareConfig().setProperties((authProps ++ dataprocProps ++ yarnProps).asJava)

      // This gives us Spark 2.0.2. See:
      //   https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions
      // Dataproc supports Spark 2.2.0, but there are no pre-packaged Hail distributions past 2.1.0. See:
      //   https://hail.is/docs/stable/getting_started.html
      .setImageVersion("1.1")
  }

  private def getMultiNodeClusterConfig(machineConfig: MachineConfig): ClusterConfig = {
    // Set the configs of the non-preemptible, primary worker nodes
    val clusterConfigWithWorkerConfigs = new ClusterConfig().setWorkerConfig(getPrimaryWorkerConfig(machineConfig))

    // If the number of preemptible workers is greater than 0, set a secondary worker config
    if (machineConfig.numberOfPreemptibleWorkers.get > 0) {
      val preemptibleWorkerConfig = new InstanceGroupConfig()
        .setIsPreemptible(true)
        .setNumInstances(machineConfig.numberOfPreemptibleWorkers.get)

      clusterConfigWithWorkerConfigs.setSecondaryWorkerConfig(preemptibleWorkerConfig)
    } else clusterConfigWithWorkerConfigs
  }

  private def getPrimaryWorkerConfig(machineConfig: MachineConfig): InstanceGroupConfig = {
    val workerDiskConfig = new DiskConfig()
      .setBootDiskSizeGb(machineConfig.workerDiskSize.get)
      .setNumLocalSsds(machineConfig.numberOfWorkerLocalSSDs.get)

    new InstanceGroupConfig()
      .setNumInstances(machineConfig.numberOfWorkers.get)
      .setMachineTypeUri(machineConfig.workerMachineType.get)
      .setDiskConfig(workerDiskConfig)
  }

  // Using the given access token, look up the corresponding UserInfo of the user
  override def getUserInfoAndExpirationFromAccessToken(accessToken: String): Future[(UserInfo, Instant)] = {
    val request = oauth2.tokeninfo().setAccessToken(accessToken)
    executeGoogleRequestAsync(GoogleProject(""), "cookie auth", request).map { tokenInfo =>
      (UserInfo(OAuth2BearerToken(accessToken), WorkbenchUserId(tokenInfo.getUserId), WorkbenchEmail(tokenInfo.getEmail), tokenInfo.getExpiresIn.toInt), Instant.now().plusSeconds(tokenInfo.getExpiresIn.toInt))
    }.recover { case CallToGoogleApiFailedException(_, _, _, _) => {
      logger.error(s"Unable to authorize token: $accessToken")
      throw AuthorizationError()
    }}
  }

  /**
    * Gets a dataproc Cluster from the API.
    */
  private def getCluster(googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[DataprocCluster] = {
    val request = dataproc.projects().regions().clusters().get(googleProject.value, defaultRegion, clusterName.value)
    executeGoogleRequestAsync(googleProject, clusterName.toString, request)
  }

  /**
    * Gets a compute Instance from the API.
    */
  private def getInstance(googleProject: GoogleProject, zone: ZoneUri, instanceName: InstanceName)(implicit executionContext: ExecutionContext): Future[Instance] = {
    val request = compute.instances().get(googleProject.value, zone.value, instanceName.value)
    executeGoogleRequestAsync(googleProject, instanceName.toString, request)
  }

  /**
    * Gets an Operation from the API.
    */
  private def getOperation(operationName: OperationName)(implicit executionContext: ExecutionContext): Future[DataprocOperation] = {
    val request = dataproc.projects().regions().operations().get(operationName.value)
    executeGoogleRequestAsync(GoogleProject("operation"), operationName.toString, request)
  }

  private def getOperationUUID(dop: DataprocOperation): UUID = {
    UUID.fromString(dop.getMetadata.get("clusterUuid").toString)
  }

  /**
    * Gets the master instance name from a dataproc cluster, with error handling.
    * @param cluster the Google dataproc cluster
    * @return error or master instance name
    */
  private def getMasterInstanceName(cluster: DataprocCluster): Option[InstanceName] = {
    for {
      config <- Option(cluster.getConfig)
      masterConfig <- Option(config.getMasterConfig)
      instanceNames <- Option(masterConfig.getInstanceNames)
      masterInstance <- instanceNames.asScala.headOption
    } yield InstanceName(masterInstance)
  }

  /**
    * Gets the zone (not to be confused with region) of a dataproc cluster, with error handling.
    * @param cluster the Google dataproc cluster
    * @return error or the master instance zone
    */
  private def getZone(cluster: DataprocCluster): Option[ZoneUri] = {
    def parseZone(zoneUri: String): ZoneUri = {
      zoneUri.lastIndexOf('/') match {
        case -1 => ZoneUri(zoneUri)
        case n => ZoneUri(zoneUri.substring(n + 1))
      }
    }

    for {
      config <- Option(cluster.getConfig)
      gceConfig <- Option(config.getGceClusterConfig)
      zoneUri <- Option(gceConfig.getZoneUri)
    } yield parseZone(zoneUri)
  }

  /**
    * Gets the public IP from a google Instance, with error handling.
    * @param instance the Google instance
    * @return error or public IP, as a String
    */
  private def getInstanceIP(instance: Instance): Option[IP] = {
    for {
      interfaces <- Option(instance.getNetworkInterfaces)
      interface <- interfaces.asScala.headOption
      accessConfigs <- Option(interface.getAccessConfigs)
      accessConfig <- accessConfigs.asScala.headOption
    } yield IP(accessConfig.getNatIP)
  }

  /**
    * Gets the staging bucket from a dataproc cluster, with error handling.
    * @param cluster the Google dataproc cluster
    * @return error or staging bucket
    */
  private def getStagingBucket(cluster: DataprocCluster): Option[GcsBucketName] = {
    for {
      config <- Option(cluster.getConfig)
      bucket <- Option(config.getConfigBucket)
    } yield GcsBucketName(bucket)
  }

  private def getInitBucketName(cluster: DataprocCluster): Option[GcsBucketName] = {
    for {
      config <- Option(cluster.getConfig)
      initAction <- config.getInitializationActions.asScala.headOption
      bucketPath <- Option(initAction.getExecutableFile)
      parsedBucketPath <- parseGcsPath(bucketPath).toOption
    } yield parsedBucketPath.bucketName
  }

  private def executeGoogleRequestAsync[A](googleProject: GoogleProject, context: String, request: AbstractGoogleClientRequest[A])(implicit executionContext: ExecutionContext): Future[A] = {
    Future {
      blocking(executeGoogleRequest(request))
    } recover {
      case e: GoogleJsonResponseException =>
        logger.error(s"Error occurred executing Google request for ${googleProject.value} / $context", e)
        throw CallToGoogleApiFailedException(googleProject, context, e.getStatusCode, e.getDetails.getMessage)
      case illegalArgumentException: IllegalArgumentException =>
        logger.error(s"Illegal argument passed to Google request for ${googleProject.value} / $context", illegalArgumentException)
        throw CallToGoogleApiFailedException(googleProject, context, StatusCodes.BadRequest.intValue, illegalArgumentException.getMessage)
    }
  }

  private def getProjectNumber(googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[Long]] = {
    val request = cloudResourceManager.projects().get(googleProject.value)
    executeGoogleRequestAsync(googleProject, "", request).map { project =>
      Option(project.getProjectNumber).map(_.toLong)
    }.recover { case CallToGoogleApiFailedException(_, _, 404, _) =>
      None
    }
  }
}
