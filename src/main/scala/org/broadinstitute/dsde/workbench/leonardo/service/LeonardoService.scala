package org.broadinstitute.dsde.workbench.leonardo.service

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.Monoid
import cats.data.{Ior, OptionT}
import cats.implicits._
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.HttpResponseException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.config.{AutoFreezeConfig, ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DataAccess, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.{Jupyter, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions._
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.Stopped
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper}
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.Retry
import slick.dbio.DBIO
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class AuthorizationError(email: Option[WorkbenchEmail] = None)
  extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is unauthorized", StatusCodes.Forbidden)

case class AuthenticationError(email: Option[WorkbenchEmail] = None)
  extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is not authenticated", StatusCodes.Unauthorized)

case class ClusterNotFoundException(googleProject: GoogleProject, clusterName: ClusterName)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} not found", StatusCodes.NotFound)

case class ClusterAlreadyExistsException(googleProject: GoogleProject, clusterName: ClusterName, status: ClusterStatus)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} already exists in ${status.toString} status", StatusCodes.Conflict)

case class ClusterCannotBeStoppedException(googleProject: GoogleProject, clusterName: ClusterName, status: ClusterStatus)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} cannot be stopped in ${status.toString} status", StatusCodes.Conflict)

case class ClusterCannotBeDeletedException(googleProject: GoogleProject, clusterName: ClusterName)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} cannot be deleted in Creating status", StatusCodes.Conflict)

case class ClusterCannotBeStartedException(googleProject: GoogleProject, clusterName: ClusterName, status: ClusterStatus)
  extends LeoException(s"Cluster ${googleProject.value}/${clusterName.value} cannot be started in ${status.toString} status", StatusCodes.Conflict)

case class ClusterCannotBeUpdatedException(cluster: Cluster)
  extends LeoException(s"Cluster ${cluster.projectNameString} cannot be updated in ${cluster.status} status", StatusCodes.Conflict)

case class ClusterMachineTypeCannotBeChangedException(cluster: Cluster)
  extends LeoException(s"Cluster ${cluster.projectNameString} in ${cluster.status} status must be stopped in order to change machine type", StatusCodes.Conflict)

case class ClusterDiskSizeCannotBeDecreasedException(cluster: Cluster)
  extends LeoException(s"Cluster ${cluster.projectNameString}: decreasing master disk size is not allowed", StatusCodes.PreconditionFailed)

case class InitializationFileException(googleProject: GoogleProject, clusterName: ClusterName, errorMessage: String)
  extends LeoException(s"Unable to process initialization files for ${googleProject.value}/${clusterName.value}. Returned message: $errorMessage", StatusCodes.Conflict)

case class BucketObjectException(gcsUri: String)
  extends LeoException(s"The provided GCS URI is invalid or unparseable: ${gcsUri}", StatusCodes.BadRequest)

case class BucketObjectAccessException(userEmail: WorkbenchEmail, gcsUri: GcsPath)
  extends LeoException(s"${userEmail.value} does not have access to ${gcsUri.toUri}", StatusCodes.Forbidden)

case class DataprocDisabledException(errorMsg: String)
  extends LeoException(s"${errorMsg}", StatusCodes.Forbidden)

case class ParseLabelsException(labelString: String)
  extends LeoException(s"Could not parse label string: $labelString. Expected format [key1=value1,key2=value2,...]", StatusCodes.BadRequest)

case class IllegalLabelKeyException(labelKey: String)
  extends LeoException(s"Labels cannot have a key of '$labelKey'", StatusCodes.NotAcceptable)

case class InvalidDataprocMachineConfigException(errorMsg: String)
  extends LeoException(s"${errorMsg}", StatusCodes.BadRequest)

class LeonardoService(protected val dataprocConfig: DataprocConfig,
                      protected val welderDao: WelderDAO,
                      protected val clusterFilesConfig: ClusterFilesConfig,
                      protected val clusterResourcesConfig: ClusterResourcesConfig,
                      protected val clusterDefaultsConfig: ClusterDefaultsConfig,
                      protected val proxyConfig: ProxyConfig,
                      protected val swaggerConfig: SwaggerConfig,
                      protected val autoFreezeConfig: AutoFreezeConfig,
                      protected val petGoogleStorageDAO: String => GoogleStorageDAO,
                      protected val dbRef: DbReference,
                      protected val authProvider: LeoAuthProvider,
                      protected val serviceAccountProvider: ServiceAccountProvider,
                      protected val bucketHelper: BucketHelper,
                      protected val clusterHelper: ClusterHelper,
                      protected val contentSecurityPolicy: String)
                     (implicit val executionContext: ExecutionContext,
                      implicit override val system: ActorSystem) extends LazyLogging with Retry {

  private val bucketPathMaxLength = 1024
  private val includeDeletedKey = "includeDeleted"

  protected def checkProjectPermission(userInfo: UserInfo, action: ProjectAction, project: GoogleProject): Future[Unit] = {
    authProvider.hasProjectPermission(userInfo, action, project) map {
      case false => throw AuthorizationError(Option(userInfo.userEmail))
      case true => ()
    }
  }

  //Throws 404 and pretends we don't even know there's a cluster there, by default.
  //If the cluster really exists and you're OK with the user knowing that, set throw401 = true.
  protected def checkClusterPermission(userInfo: UserInfo, action: NotebookClusterAction, cluster: Cluster, throw403: Boolean = false): Future[Unit] = {
    authProvider.hasNotebookClusterPermission(userInfo, action, cluster.googleProject, cluster.clusterName) map {
      case false =>
        logger.warn(s"User ${userInfo.userEmail} does not have the notebook permission for " +
          s"${cluster.googleProject}/${cluster.clusterName}")

        if (throw403)
          throw AuthorizationError(Option(userInfo.userEmail))
        else
          throw ClusterNotFoundException(cluster.googleProject, cluster.clusterName)
      case true => ()
    }
  }

  // We complete the API response without waiting for the cluster to be created
  // on the Google Dataproc side, which happens asynchronously to the request
  def processClusterCreationRequest(userInfo: UserInfo,
                                    googleProject: GoogleProject,
                                    clusterName: ClusterName,
                                    clusterRequest: ClusterRequest): Future[Cluster] = {
    for {
      _ <- checkProjectPermission(userInfo, CreateClusters, googleProject)

      // Grab the service accounts from serviceAccountProvider for use later
      clusterServiceAccountOpt <- serviceAccountProvider.getClusterServiceAccount(userInfo, googleProject)
      notebookServiceAccountOpt <- serviceAccountProvider.getNotebookServiceAccount(userInfo, googleProject)
      serviceAccountInfo = ServiceAccountInfo(clusterServiceAccountOpt, notebookServiceAccountOpt)

      cluster <- initiateClusterCreation(
        userInfo.userEmail, serviceAccountInfo, googleProject, clusterName, clusterRequest)
    } yield cluster
  }

  // If the google project does not have an active cluster with the given name,
  // we start creating one.
  private def initiateClusterCreation(userEmail: WorkbenchEmail,
                              serviceAccountInfo: ServiceAccountInfo,
                              googleProject: GoogleProject,
                              clusterName: ClusterName,
                              clusterRequest: ClusterRequest): Future[Cluster] = {
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getActiveClusterByNameMinimal(googleProject, clusterName)
    } flatMap {
      case Some(existingCluster) =>
        throw ClusterAlreadyExistsException(googleProject, clusterName, existingCluster.status)
      case None =>
        stageClusterCreation(userEmail, serviceAccountInfo, googleProject, clusterName, clusterRequest)
    }
  }

  private def stageClusterCreation(userEmail: WorkbenchEmail,
                                   serviceAccountInfo: ServiceAccountInfo,
                                   googleProject: GoogleProject,
                                   clusterName: ClusterName,
                                   clusterRequest: ClusterRequest): Future[Cluster] = {
    val augmentedClusterRequest = augmentClusterRequest(serviceAccountInfo, googleProject, clusterName, userEmail, clusterRequest)
    val clusterImages = getClusterImages(clusterRequest)
    val machineConfig = MachineConfigOps.create(clusterRequest.machineConfig, clusterDefaultsConfig)
    val autopauseThreshold = calculateAutopauseThreshold(
      clusterRequest.autopause, clusterRequest.autopauseThreshold)
    val clusterScopes = if (clusterRequest.scopes.isEmpty) dataprocConfig.defaultScopes else clusterRequest.scopes
    val initialClusterToSave = Cluster.createInitial(
      augmentedClusterRequest, userEmail, clusterName, googleProject,
      serviceAccountInfo, machineConfig, dataprocConfig.clusterUrlBase, autopauseThreshold, clusterScopes,
      clusterImages = clusterImages)

    // Validate that the Jupyter extension URIs and Jupyter user script URI are valid URIs and reference real GCS objects
    // and if so, save the cluster creation request parameters in DB
    for {
      _ <- validateClusterRequestBucketObjectUri(userEmail, googleProject, augmentedClusterRequest)
      _ <- authProvider.notifyClusterCreated(userEmail, googleProject, clusterName)
      cluster <- dbRef.inTransaction { _.clusterQuery.save(initialClusterToSave) }
    } yield cluster
  }


  //throws 404 if nonexistent or no permissions
  def getActiveClusterDetails(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName): Future[Cluster] = {
    for {
      cluster <- internalGetActiveClusterDetails(googleProject, clusterName) //throws 404 if nonexistent
      _ <- checkClusterPermission(userInfo, GetClusterStatus, cluster) //throws 404 if no auth
    } yield { cluster }
  }

  def internalGetActiveClusterDetails(googleProject: GoogleProject, clusterName: ClusterName): Future[Cluster] = {
    dbRef.inTransaction { dataAccess =>
      getActiveCluster(googleProject, clusterName, dataAccess)
    }
  }

  def updateCluster(userInfo: UserInfo,
                    googleProject: GoogleProject,
                    clusterName: ClusterName,
                    clusterRequest: ClusterRequest): Future[Cluster] = {
    for {
      cluster <- internalGetActiveClusterDetails(googleProject, clusterName) //throws 404 if nonexistent
      _ <- checkClusterPermission(userInfo, ModifyCluster, cluster) //throws 404 if no auth
      updatedCluster <- internalUpdateCluster(cluster, clusterRequest)
    } yield updatedCluster
  }

  def internalUpdateCluster(existingCluster: Cluster, clusterRequest: ClusterRequest) = {
    implicit val booleanSumMonoidInstance = new Monoid[Boolean] {
      def empty = false
      def combine(a: Boolean, b: Boolean) = a || b
    }

    if (existingCluster.status.isUpdatable) {
      for {
        autopauseChanged <- maybeUpdateAutopauseThreshold(existingCluster, clusterRequest.autopause, clusterRequest.autopauseThreshold).attempt

        clusterResized <- maybeResizeCluster(existingCluster, clusterRequest.machineConfig).attempt

        masterMachineTypeChanged <- maybeChangeMasterMachineType(existingCluster, clusterRequest.machineConfig).attempt

        masterDiskSizeChanged <- maybeChangeMasterDiskSize(existingCluster, clusterRequest.machineConfig).attempt

        // Note: only resizing a cluster triggers a status transition to Updating
        (errors, shouldUpdate) = List(
          autopauseChanged.map(_ => false),
          clusterResized,
          masterMachineTypeChanged.map(_ => false),
          masterDiskSizeChanged.map(_ => false)
        ).separate

        // Set the cluster status to Updating if the cluster was resized
        _ <- if (shouldUpdate.combineAll) {
          dbRef.inTransaction { _.clusterQuery.updateClusterStatus(existingCluster.id, ClusterStatus.Updating) }.void
        } else Future.unit

        cluster <- errors match {
          case Nil => internalGetActiveClusterDetails(existingCluster.googleProject, existingCluster.clusterName)
          // Just return the first error; we don't have a great mechanism to return all errors
          case h :: _ => Future.failed(h)
        }
      } yield cluster

    } else Future.failed(ClusterCannotBeUpdatedException(existingCluster))
  }

  private def getUpdatedValueIfChanged[A](existing: Option[A], updated: Option[A]): Option[A] = {
    (existing, updated) match {
      case (None, Some(0)) => None //An updated value of 0 is considered the same as None to prevent google APIs from complaining
      case (_, Some(x)) if updated != existing => Some(x)
      case _ => None
    }
  }

  def maybeUpdateAutopauseThreshold(existingCluster: Cluster, autopause: Option[Boolean], autopauseThreshold: Option[Int]): Future[Boolean] = {
    val updatedAutopauseThresholdOpt = getUpdatedValueIfChanged(Option(existingCluster.autopauseThreshold), Option(calculateAutopauseThreshold(autopause, autopauseThreshold)))
    updatedAutopauseThresholdOpt match {
      case Some(updatedAutopauseThreshold) =>
        logger.info(s"Changing autopause threshold for cluster ${existingCluster.projectNameString}")

        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.updateAutopauseThreshold(existingCluster.id, updatedAutopauseThreshold)
        }.as(true)

      case None => Future.successful(false)
    }
  }

  //returns true if cluster was resized, otherwise returns false
  def maybeResizeCluster(existingCluster: Cluster, machineConfigOpt: Option[MachineConfig]): Future[Boolean] = {
    //machineConfig.numberOfPreemtible undefined, and a 0 is passed in
    //
    val updatedNumWorkersAndPreemptiblesOpt = machineConfigOpt.flatMap { machineConfig =>
      Ior.fromOptions(
        getUpdatedValueIfChanged(existingCluster.machineConfig.numberOfWorkers, machineConfig.numberOfWorkers),
        getUpdatedValueIfChanged(existingCluster.machineConfig.numberOfPreemptibleWorkers, machineConfig.numberOfPreemptibleWorkers))
    }

    updatedNumWorkersAndPreemptiblesOpt match {
      case Some(updatedNumWorkersAndPreemptibles) =>
        logger.info(s"New machine config present. Resizing cluster '${existingCluster.projectNameString}'...")

        for {
          _ <- clusterHelper.resizeCluster(existingCluster, updatedNumWorkersAndPreemptibles.left, updatedNumWorkersAndPreemptibles.right)

          // Update the DB
          _ <- dbRef.inTransaction { dataAccess =>
            updatedNumWorkersAndPreemptibles.fold(
              a => dataAccess.clusterQuery.updateNumberOfWorkers(existingCluster.id, a),
              a => dataAccess.clusterQuery.updateNumberOfPreemptibleWorkers(existingCluster.id, Option(a)),
              (a, b) => dataAccess.clusterQuery.updateNumberOfWorkers(existingCluster.id, a)
                .flatMap(_ =>  dataAccess.clusterQuery.updateNumberOfPreemptibleWorkers(existingCluster.id, Option(b)))
            )
          }
        } yield true

      case None => Future.successful(false)
    }
  }

  def maybeChangeMasterMachineType(existingCluster: Cluster, machineConfigOpt: Option[MachineConfig]): Future[Boolean] = {
    val updatedMasterMachineTypeOpt = machineConfigOpt.flatMap { machineConfig =>
      getUpdatedValueIfChanged(existingCluster.machineConfig.masterMachineType, machineConfig.masterMachineType)
    }

    updatedMasterMachineTypeOpt match {
      // Note: instance must be stopped in order to change machine type
      // TODO future enchancement: add capability to Leo to manage stop/update/restart transitions itself.
      case Some(updatedMasterMachineType) if existingCluster.status == Stopped =>
        logger.info(s"New machine config present. Changing machine type to ${updatedMasterMachineType} for cluster ${existingCluster.projectNameString}...")

        for {
          _ <- clusterHelper.setMasterMachineType(existingCluster, MachineType(updatedMasterMachineType))
          _ <- dbRef.inTransaction { _.clusterQuery.updateMasterMachineType(existingCluster.id, MachineType(updatedMasterMachineType)) }
        } yield true

      case Some(_) =>
        Future.failed(ClusterMachineTypeCannotBeChangedException(existingCluster))

      case None =>
        Future.successful(false)
    }
  }

  def maybeChangeMasterDiskSize(existingCluster: Cluster, machineConfigOpt: Option[MachineConfig]): Future[Boolean] = {
    val updatedMasterDiskSizeOpt = machineConfigOpt.flatMap { machineConfig =>
      getUpdatedValueIfChanged(existingCluster.machineConfig.masterDiskSize, machineConfig.masterDiskSize)
    }

    // Note: GCE allows you to increase a persistent disk, but not decrease. Throw an exception if the user tries to decrease their disk.
    val diskSizeIncreased = (newSize: Int) => existingCluster.machineConfig.masterDiskSize.exists(_ < newSize)

    updatedMasterDiskSizeOpt match {
      case Some(updatedMasterDiskSize) if diskSizeIncreased(updatedMasterDiskSize) =>
        logger.info(s"New machine config present. Changing master disk size to $updatedMasterDiskSize GB for cluster ${existingCluster.projectNameString}...")

        for {
          _ <- clusterHelper.updateMasterDiskSize(existingCluster, updatedMasterDiskSize)
          _ <- dbRef.inTransaction { _.clusterQuery.updateMasterDiskSize(existingCluster.id, updatedMasterDiskSize) }
        } yield true

      case Some(_) =>
        Future.failed(ClusterDiskSizeCannotBeDecreasedException(existingCluster))

      case None =>
        Future.successful(false)
    }
  }

  def deleteCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 401 is appropriate if you can't actually destroy it
      _ <- checkClusterPermission(userInfo,  DeleteCluster, cluster, throw403 = true)

      _ <- internalDeleteCluster(userInfo.userEmail, cluster)
    } yield ()
  }

  //NOTE: This function MUST ALWAYS complete ALL steps. i.e. if deleting thing1 fails, it must still proceed to delete thing2
  def internalDeleteCluster(userEmail: WorkbenchEmail, cluster: Cluster): Future[Unit] = {
    if (cluster.status.isDeletable) {
      for {
        // Delete the notebook service account key in Google, if present
        keyOpt <- dbRef.inTransaction { _.clusterQuery.getServiceAccountKeyId(cluster.googleProject, cluster.clusterName) }

        // Delete the cluster in Google
        _ <- clusterHelper.deleteCluster(cluster, keyOpt)

        // Change the cluster status to Deleting in the database
        // Note this also changes the instance status to Deleting
        _ <- dbRef.inTransaction(dataAccess => dataAccess.clusterQuery.markPendingDeletion(cluster.id))
      } yield ()
    } else if (cluster.status == ClusterStatus.Creating) {
      Future.failed(ClusterCannotBeDeletedException(cluster.googleProject, cluster.clusterName))
    } else Future.unit
  }

  def stopCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 401 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo, StopStartCluster, cluster, throw403 = true)

      _ <- internalStopCluster(cluster)
    } yield ()
  }

  def internalStopCluster(cluster: Cluster): Future[Unit] = {
    if (cluster.status.isStoppable) {
      for {
        _ <- if(cluster.welderEnabled) {
          welderDao.flushCache(cluster.googleProject, cluster.clusterName).handleError(e => logger.error(s"fail to flush welder cache for ${cluster}"))
        }

        _ <- clusterHelper.stopCluster(cluster)

        // Update the cluster status to Stopping
        _ <- dbRef.inTransaction { _.clusterQuery.setToStopping(cluster.id) }
      } yield ()

    } else Future.failed(ClusterCannotBeStoppedException(cluster.googleProject, cluster.clusterName, cluster.status))
  }

  def startCluster(userInfo: UserInfo, googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    for {
      //throws 404 if no permissions
      cluster <- getActiveClusterDetails(userInfo, googleProject, clusterName)

      //if you've got to here you at least have GetClusterDetails permissions so a 401 is appropriate if you can't actually stop it
      _ <- checkClusterPermission(userInfo, StopStartCluster, cluster, throw403 = true)

      _ <- internalStartCluster(userInfo.userEmail, cluster)
    } yield ()
  }

  def internalStartCluster(userEmail: WorkbenchEmail, cluster: Cluster): Future[Unit] = {
    if (cluster.status.isStartable) {
      for {
        _ <- clusterHelper.startCluster(cluster)

        // Update the cluster status to Starting
        _ <- dbRef.inTransaction { _.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Starting) }
      } yield ()

    } else Future.failed(ClusterCannotBeStartedException(cluster.googleProject, cluster.clusterName, cluster.status))
  }

  def listClusters(userInfo: UserInfo, params: LabelMap, googleProjectOpt: Option[GoogleProject] = None): Future[Seq[Cluster]] = {
    for {
      paramMap <- processListClustersParameters(params)
      clusterList <- dbRef.inTransaction { da => da.clusterQuery.listByLabels(paramMap._1, paramMap._2, googleProjectOpt) }
      samVisibleClusters <- authProvider.filterUserVisibleClusters(userInfo, clusterList.map(c => (c.googleProject, c.clusterName)).toList)
    } yield {
      // Making the assumption that users will always be able to access clusters that they create
      // Fix for https://github.com/DataBiosphere/leonardo/issues/821
      val visibleClusters = samVisibleClusters:::clusterList.filter(_.auditInfo.creator == userInfo.userEmail).map(c => (c.googleProject, c.clusterName)).toList
      val visibleClustersSet = visibleClusters.toSet
      clusterList.filter(c => visibleClustersSet.contains((c.googleProject, c.clusterName)))
    }
  }

  private[service] def getActiveCluster(googleProject: GoogleProject, clusterName: ClusterName, dataAccess: DataAccess): DBIO[Cluster] = {
    dataAccess.clusterQuery.getActiveClusterByName(googleProject, clusterName) flatMap {
      case None => throw ClusterNotFoundException(googleProject, clusterName)
      case Some(cluster) => DBIO.successful(cluster)
    }
  }

  private def calculateAutopauseThreshold(autopause: Option[Boolean], autopauseThreshold: Option[Int]): Int = {
    autopause match {
      case None =>
        autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
      case Some(false) =>
        autoPauseOffValue
      case _ =>
        if (autopauseThreshold.isEmpty) autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
        else Math.max(autoPauseOffValue, autopauseThreshold.get)
    }
  }

//  private def persistErrorInDb(e: Throwable,
//                               clusterName: ClusterName,
//                               clusterId: Long,
//                               googleProject: GoogleProject): Future[Unit] = {
//    val errorMessage = e match {
//      case leoEx: LeoException =>
//        ErrorReport.loggableString(leoEx.toErrorReport)
//      case _ =>
//        s"Asynchronous creation of cluster '$clusterName' on Google project " +
//          s"'$googleProject' failed due to '${e.toString}'."
//    }
//
//    // TODO Make errorCode field nullable in ClusterErrorComponent and pass None below
//    // See https://github.com/DataBiosphere/leonardo/issues/512
//    val dummyErrorCode = -1
//
//    val errorInfo = ClusterError(errorMessage, dummyErrorCode, Instant.now)
//
//    dbRef.inTransaction { dataAccess =>
//      for {
//        _ <- dataAccess.clusterQuery.updateClusterStatus(clusterId, ClusterStatus.Error)
//        _ <- dataAccess.clusterErrorQuery.save(clusterId, errorInfo)
//      } yield ()
//    }
//  }

  private def validateClusterRequestBucketObjectUri(userEmail: WorkbenchEmail, googleProject: GoogleProject, clusterRequest: ClusterRequest)
                                                   (implicit executionContext: ExecutionContext): Future[Unit] = {
    val transformed = for {
      // Get a pet token from Sam. If we can't get a token, we won't do validation but won't fail cluster creation.
      petToken <- OptionT(serviceAccountProvider.getAccessToken(userEmail, googleProject).recover { case e =>
        logger.warn(s"Could not acquire pet service account access token for user ${userEmail.value} in project $googleProject. " +
          s"Skipping validation of bucket objects in the cluster request.", e)
        None
      })

      // Validate the user script URI
      _ <- clusterRequest.jupyterUserScriptUri match {
        case Some(userScriptUri) => OptionT.liftF[Future, Unit](validateBucketObjectUri(userEmail, petToken, userScriptUri.toUri))
        case None => OptionT.pure[Future](())
      }

      // Validate the extension URIs
      _ <- clusterRequest.userJupyterExtensionConfig match {
        case Some(config) =>
          val extensionsToValidate = (config.nbExtensions.values ++ config.serverExtensions.values ++ config.combinedExtensions.values).filter(_.startsWith("gs://"))
          OptionT.liftF(Future.traverse(extensionsToValidate)(x => validateBucketObjectUri(userEmail, petToken, x)))
        case None => OptionT.pure[Future](())
      }
    } yield ()

    // Because of how OptionT works, `transformed.value` returns a Future[Option[Unit]]. `void` converts this to a Future[Unit].
    transformed.value.void
  }

  private[service] def validateBucketObjectUri(userEmail: WorkbenchEmail, userToken: String, gcsUri: String)
                                              (implicit executionContext: ExecutionContext): Future[Unit] = {
    logger.debug(s"Validating user [${userEmail.value}] has access to bucket object $gcsUri")
    val gcsUriOpt = parseGcsPath(gcsUri)
    gcsUriOpt match {
      case Left(_) => Future.failed(BucketObjectException(gcsUri))
      case Right(gcsPath) if gcsPath.toUri.length > bucketPathMaxLength => Future.failed(BucketObjectException(gcsUri))
      case Right(gcsPath) =>
        // Retry 401s from Google here because they can be thrown spuriously with valid credentials.
        // See https://github.com/DataBiosphere/leonardo/issues/460
        // Note GoogleStorageDAO already retries 500 and other errors internally, so we just need to catch 401s here.
        // We might think about moving the retry-on-401 logic inside GoogleStorageDAO.
        val errorMessage = s"GCS object validation failed for user [${userEmail.value}] and token [$userToken] and object [${gcsUri}]"
        val gcsFuture: Future[Boolean] = retryUntilSuccessOrTimeout(whenGoogle401, errorMessage)(interval = 1 second, timeout = 3 seconds) { () =>
          petGoogleStorageDAO(userToken).objectExists(gcsPath.bucketName, gcsPath.objectName)
        }
        gcsFuture.map {
          case true => ()
          case false => throw BucketObjectException(gcsPath.toUri)
        } recover {
          case e: HttpResponseException if e.getStatusCode == StatusCodes.Forbidden.intValue =>
            logger.error(s"User ${userEmail.value} does not have access to ${gcsPath.bucketName} / ${gcsPath.objectName}")
            throw BucketObjectAccessException(userEmail, gcsPath)
          case e if whenGoogle401(e) =>
            logger.warn(s"Could not validate object [${gcsUri}] as user [${userEmail.value}]", e)
            ()
        }
    }
  }

  private def whenGoogle401(t: Throwable): Boolean = t match {
    case g: GoogleJsonResponseException if g.getStatusCode == StatusCodes.Unauthorized.intValue => true
    case _ => false
  }

  private[service] def processListClustersParameters(params: LabelMap): Future[(LabelMap, Boolean)] = {
    Future {
      params.get(includeDeletedKey) match {
        case Some(includeDeletedValue) => (processLabelMap(params - includeDeletedKey), includeDeletedValue.toBoolean)
        case None => (processLabelMap(params), false)
      }
    }
  }

  /**
    * There are 2 styles of passing labels to the list clusters endpoint:
    *
    * 1. As top-level query string parameters: GET /api/clusters?foo=bar&baz=biz
    * 2. Using the _labels query string parameter: GET /api/clusters?_labels=foo%3Dbar,baz%3Dbiz
    *
    * The latter style exists because Swagger doesn't provide a way to specify free-form query string
    * params. This method handles both styles, and returns a Map[String, String] representing the labels.
    *
    * Note that style 2 takes precedence: if _labels is present on the query string, any additional
    * parameters are ignored.
    *
    * @param params raw query string params
    * @return a Map[String, String] representing the labels
    */
  private[service] def processLabelMap(params: LabelMap): LabelMap = {
    params.get("_labels") match {
      case Some(extraLabels) =>
        extraLabels.split(',').foldLeft(Map.empty[String, String]) { (r, c) =>
          c.split('=') match {
            case Array(key, value) => r + (key -> value)
            case _ => throw ParseLabelsException(extraLabels)
          }
        }
      case None => params
    }
  }

  private[service] def getExtensionConfig(clusterRequest: ClusterRequest): Option[UserJupyterExtensionConfig] = {
    // legacy extension param
    (clusterRequest.jupyterExtensionUri, clusterRequest.userJupyterExtensionConfig) match {
      case (Some(legacy), Some(extensionConfig)) =>
        Some(extensionConfig.copy(nbExtensions = extensionConfig.nbExtensions ++ Map("notebookExtension" -> legacy.toUri)))
      case (Some(legacy), None) =>
        Some(UserJupyterExtensionConfig(Map("notebookExtension" -> legacy.toUri)))
      case (None, extensionConfigOpt) =>
        extensionConfigOpt
    }
  }

  private[service] def augmentClusterRequest(serviceAccountInfo: ServiceAccountInfo, googleProject: GoogleProject, clusterName: ClusterName, userEmail: WorkbenchEmail, clusterRequest: ClusterRequest) = {
    clusterRequest.copy(
      userJupyterExtensionConfig = getExtensionConfig(clusterRequest),
      labels = getClusterLabels(serviceAccountInfo, googleProject, clusterName, userEmail, clusterRequest)
    )
  }

  private[service] def getClusterLabels(serviceAccountInfo: ServiceAccountInfo,
                                        googleProject: GoogleProject,
                                        clusterName: ClusterName,
                                        creator: WorkbenchEmail,
                                        clusterRequest: ClusterRequest): Map[String, String] = {
    // create a LabelMap of default labels
    val defaultLabels = DefaultLabels(clusterName, googleProject, creator,
      serviceAccountInfo.clusterServiceAccount, serviceAccountInfo.notebookServiceAccount, clusterRequest.jupyterUserScriptUri)
      .toJson.asJsObject.fields.mapValues(labelValue => labelValue.convertTo[String])

    // combine default and given labels and add labels for extensions
    val allLabels = clusterRequest.labels ++ defaultLabels ++
      clusterRequest.userJupyterExtensionConfig.map(_.asLabels).getOrElse(Map.empty)

    // check the labels do not contain forbidden keys
    if (allLabels.contains(includeDeletedKey))
      throw IllegalLabelKeyException(includeDeletedKey)
    else clusterRequest
      allLabels
  }

  private[service] def getClusterImages(clusterRequest: ClusterRequest): Set[ClusterImage] = {
    val now = Instant.now

    //If welder is enabled for this cluster, we need to ensure that an image is chosen.
    //We will use the client-supplied image, if present, otherwise we will use a default.
    //If welder is not enabled, we won't use any image.
    //Eventually welder will be enabled for all clusters and this will be way cleaner.
    val welderImageOpt: Option[ClusterImage] = if(clusterRequest.enableWelder.getOrElse(false)) {
      val i = clusterRequest.welderDockerImage.getOrElse(dataprocConfig.welderDockerImage)
      Some(ClusterImage(Welder, i, now))
    } else None

    // Note: Jupyter image is not currently optional
    val jupyterImage: ClusterImage = ClusterImage(Jupyter,
      clusterRequest.jupyterDockerImage.getOrElse(dataprocConfig.dataprocDockerImage), now)

    // Optional RStudio image
    val rstudioImageOpt: Option[ClusterImage] = clusterRequest.rstudioDockerImage.map(i => ClusterImage(RStudio, i, now))

    Set(welderImageOpt, Some(jupyterImage), rstudioImageOpt).flatten
  }

}
