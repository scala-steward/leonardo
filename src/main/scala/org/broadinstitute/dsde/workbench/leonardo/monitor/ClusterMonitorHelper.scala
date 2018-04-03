package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import akka.actor.ActorRef
import akka.util.Timeout
import akka.pattern.ask
import cats.data.OptionT
import cats.implicits._
import cats.effect._
import cats.instances.SetInstances
import com.typesafe.scalalogging.LazyLogging
import fs2._
import fs2.async.mutable.Queue
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.{ClusterReady, GetClusterResponse, ProcessReadyCluster}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.{Deleted, Deleting}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor.{ClusterMonitorMessage, ShutdownActor}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterDeleted
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotReadyException
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import slick.dbio.DBIOAction

import scala.collection.immutable.Set
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Created by rtitle on 3/29/18.
  */
class ClusterMonitorHelper(dbRef: DbReference, gdDAO: GoogleDataprocDAO, googleComputeDAO: GoogleComputeDAO, googleStorageDAO: GoogleStorageDAO, googleIamDAO: GoogleIamDAO, clusterDnsCache: ActorRef, dataprocConfig: DataprocConfig, monitorConfig: MonitorConfig, authProvider: LeoAuthProvider)(implicit executionContext: ExecutionContext) extends LazyLogging {

  case class GoogleState(clusterStatus: ClusterStatus, instances: Set[Instance], masterIp: Option[IP], errorDetails: Option[ClusterErrorDetails])

  def monitorCreation(cluster: Cluster): Stream[IO, Boolean] = {

    val beginLog = Stream.eval_[IO, Unit] { IO(logger.info(s"Monitoring cluster ${cluster.projectNameString} for initialization")) }

    val pollResult = clusterStatusAndInstances(cluster)
      .evalMap { g => persistInstances(cluster, g.instances).asIO.map(_ => g) }
      .evalMap { g => getErrorDetails(cluster, g) }
      .map { getMasterIp }
      .takeThrough { g => clusterIsRunning(g) || clusterIsError(g) }
      .last

    (beginLog ++ pollResult).flatMap {
      case Some(g: GoogleState) if clusterIsRunning(g) =>
        Stream.eval(handleReadyCluster(cluster, g.masterIp.get, g.instances).asIO.map(_ => false))

      case Some(g: GoogleState) if clusterIsError(g) =>
        Stream.eval(handleFailedCluster(cluster, g.errorDetails.get, g.instances).asIO)

      case s =>
        Stream.eval(IO.raiseError(new Exception(s"Stopping monitoring on cluster ${cluster.projectNameString} with state $s")))

    }
  }

  def monitorStart(cluster: Cluster): IO[Unit] = {
    monitorCreation(cluster).compile.drain
  }

  def monitorCreationWithRetry(createdCluster: Cluster, retryCluster: IO[Cluster]): IO[Unit] = {
    val clusters = (Stream.emit(createdCluster) ++ Stream.eval(retryCluster).repeat)

    clusters
      .flatMap { c => monitorCreation(c) }
      .take(10)
      .takeWhile(_ == true)
      .last
      .evalMap {
        case Some(false) => IO(())
        case _ => IO.raiseError(new Exception(s"Stopping retrying cluster creation for cluster ${createdCluster.projectNameString} after 10 retries"))
      }
      .compile.drain
  }

  def monitorDeletion(cluster: Cluster): IO[Unit] = {
    val beginLog = Stream.eval_[IO, Unit] { IO(logger.info(s"Monitoring cluster ${cluster.projectNameString} for deletion")) }

    val pollResult = clusterStatusAndInstances(cluster)
      .takeThrough { clusterIsDeleted }
      .last

    (beginLog ++ pollResult).flatMap {
      case Some(g: GoogleState) if clusterIsDeleted(g) =>
        Stream.eval(handleDeletedCluster(cluster).asIO)

      case s =>
        Stream.eval(IO.raiseError(new Exception(s"stopping monitoring on cluster ${cluster.projectNameString} with state $s")))

    }.compile.drain
  }

  def monitorStop(cluster: Cluster): IO[Unit] = {
    val beginLog = Stream.eval_[IO, Unit] { IO(logger.info(s"Monitor cluster ${cluster.projectNameString} for stop")) }

    val pollResult = clusterStatusAndInstances(cluster)
      .takeThrough { clusterIsStopped }
      .last

    (beginLog ++ pollResult).flatMap {
      case Some(g: GoogleState) if clusterIsStopped(g) =>
        Stream.eval(handleStoppedCluster(cluster, g.instances).asIO)

      case s =>
        Stream.eval(IO.raiseError(new Exception(s"stopping monitoring on cluster ${cluster.projectNameString} with state $s")))

    }.compile.drain
  }

  def clusterIsRunning(googleState: GoogleState): Boolean = {
    googleState.clusterStatus == ClusterStatus.Running &&
      googleState.instances.forall(_.status == InstanceStatus.Running) &&
      googleState.masterIp.isDefined
  }

  def clusterIsError(googleState: GoogleState): Boolean = {
    googleState.clusterStatus == ClusterStatus.Error && googleState.errorDetails.isDefined
  }

  def clusterIsStopped(googleState: GoogleState): Boolean = {
    googleState.instances.forall(i => i.status == InstanceStatus.Stopped || i.status == InstanceStatus.Terminated)
  }

  def clusterIsDeleted(googleState: GoogleState): Boolean = {
    googleState.clusterStatus == ClusterStatus.Deleted
  }

  def clusterStatusAndInstances(cluster: Cluster): Stream[IO, GoogleState] = {
    val clusterStatus = Stream.eval(gdDAO.getClusterStatus(cluster.googleProject, cluster.clusterName).asIO)
    val instances = Stream.eval(getClusterInstances(cluster).asIO)

    val zipped = (clusterStatus zip instances).map { case (status, instances) =>
      GoogleState(status, instances, None, None)
    }

    val repeated = scheduler flatMap { s =>
      (zipped ++ s.sleep_[IO](15 seconds)).repeat
    }

    repeated
      .evalMap { g =>
        IO {
          logger.info(s"Cluster ${cluster.projectNameString} is not ready yet (cluster status = ${g.clusterStatus}, instance statuses = ${g.instances.map(_.status)}). Checking again in ${monitorConfig.pollPeriod.toString}.")
        }.map(_ => g)
      }
      .take(10)
  }

  ///////////

  private def handleFailedCluster(cluster: Cluster, errorDetails: ClusterErrorDetails, instances: Set[Instance]): Future[Boolean] = {
    val deleteFuture = Future.sequence(Seq(
      // Delete the cluster in Google
      gdDAO.deleteCluster(cluster.googleProject, cluster.clusterName),
      // Remove the service account key in Google, if present.
      // Only happens if the cluster was NOT created with the pet service account.
      removeServiceAccountKey(cluster),
      // create or update instances in the DB
      persistInstances(cluster, instances),
      // save cluster errors to the DB
      dbRef.inTransaction { dataAccess =>
        val clusterId = dataAccess.clusterQuery.getIdByGoogleId(cluster.googleId)
        clusterId flatMap {
          case Some(a) => dataAccess.clusterErrorQuery.save(a, ClusterError(errorDetails.message.getOrElse("Error not available"), errorDetails.code, Instant.now))
          case None => {
            logger.warn(s"Could not find Id for Cluster ${cluster.projectNameString}  with google cluster ID ${cluster.googleId}.")
            DBIOAction.successful(0)
          }
        }
      }
    ))

    deleteFuture.flatMap { _ =>
      // Decide if we should try recreating the cluster
      if (shouldRecreateCluster(errorDetails.code, errorDetails.message)) {
        // Update the database record to Deleting, shutdown this actor, and register a callback message
        // to the supervisor telling it to recreate the cluster.
        logger.info(s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails. Attempting to recreate...")
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.markPendingDeletion(cluster.googleId)
        } map { _ =>
          true
        }
      } else {
        // Update the database record to Error and shutdown this actor.
        logger.warn(s"Cluster ${cluster.projectNameString} is in an error state with $errorDetails'. Unable to recreate cluster.")
        for {
        // update the cluster status to Error
          _ <- dbRef.inTransaction { _.clusterQuery.updateClusterStatus(cluster.googleId, ClusterStatus.Error) }
          // Remove the Dataproc Worker IAM role for the pet service account
          // Only happens if the cluster was created with the pet service account.
          _ <-  removeIamRolesForUser(cluster)
        } yield false
      }
    }
  }

  private def handleReadyCluster(cluster: Cluster, publicIp: IP, instances: Set[Instance]): Future[Unit] = {
    for {
      // Delete the init bucket
      _ <- deleteInitBucket(cluster)
      // Remove credentials from instance metadata.
      // Only happens if an notebook service account was used.
      _ <- removeCredentialsFromMetadata(cluster)
      // Ensure the cluster is ready for proxying but updating the IP -> DNS cache
      _ <- ensureClusterReadyForProxying(cluster, publicIp)
      // create or update instances in the DB
      _ <- persistInstances(cluster, instances)
      // update DB after auth futures finish
      _ <- dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.setToRunning(cluster.googleId, publicIp)
      }
      // Remove the Dataproc Worker IAM role for the cluster service account.
      // Only happens if the cluster was created with a service account other
      // than the compute engine default service account.
      _ <- removeIamRolesForUser(cluster)
    } yield {
      // Finally pipe a shutdown message to this actor
      logger.info(s"Cluster ${cluster.googleProject}/${cluster.clusterName} is ready for use!")
    }
  }

  private def handleDeletedCluster(cluster: Cluster): Future[Unit] = {
    logger.info(s"Cluster ${cluster.projectNameString} has been deleted.")

    for {
      _ <- dbRef.inTransaction { dataAccess =>
        dataAccess.clusterQuery.completeDeletion(cluster.googleId)
      }
      _ <- authProvider.notifyClusterDeleted(cluster.creator, cluster.creator, cluster.googleProject, cluster.clusterName)
    } yield ()
  }


  /**
    * Handles a dataproc cluster which has been stopped.
    * We update the status to Stopped in the database and shut down this actor.
    * @return ShutdownActor
    */
  private def handleStoppedCluster(cluster: Cluster, instances: Set[Instance]): Future[ClusterMonitorMessage] = {
    logger.info(s"Cluster ${cluster.projectNameString} has been stopped.")

    for {
    // create or update instances in the DB
      _ <- persistInstances(cluster, instances)
      // this sets the cluster status to stopped and clears the cluster IP
      _ <- dbRef.inTransaction { _.clusterQuery.setToStopped(cluster.googleId) }
    } yield ShutdownActor()
  }

  private[sevice] def handleClusterCreationError(throwable: Throwable, googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Future[Unit] = {
    logger.error(s"Cluster creation failed in Google for ${googleProject.value} / ${clusterName.value}. Cleaning up resources in Google...")

    // Clean up resources in Google

    val deleteInitBucketFuture = deleteInitBucket(initBucketName, googleProject, clusterName) map { _ =>
      logger.info(s"Successfully deleted init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}")
    } recover { case e =>
      logger.error(s"Failed to delete init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}", e)
    }

    // Don't delete the staging bucket so the user can see error logs.

    val deleteClusterFuture = gdDAO.deleteCluster(googleProject, clusterName) map { _ =>
      logger.info(s"Successfully deleted cluster ${googleProject.value} / ${clusterName.value}")
    } recover { case e =>
      logger.error(s"Failed to delete cluster ${googleProject.value} / ${clusterName.value}", e)
    }

    val deleteServiceAccountKeyFuture =  removeServiceAccountKey(googleProject, clusterName, serviceAccountInfo.notebookServiceAccount) map { _ =>
      logger.info(s"Successfully deleted service account key for ${serviceAccountInfo.notebookServiceAccount}")
    } recover { case e =>
      logger.error(s"Failed to delete service account key for ${serviceAccountInfo.notebookServiceAccount}", e)
    }

    Future.sequence(Seq(deleteInitBucketFuture, deleteClusterFuture, deleteServiceAccountKeyFuture)).void
  }

  private def shouldRecreateCluster(code: Int, message: Option[String]): Boolean = {
    // TODO: potentially add more checks here as we learn which errors are recoverable
    monitorConfig.recreateCluster && (code == Code.UNKNOWN.value)
  }

  private def removeServiceAccountKey(googleProject: GoogleProject, clusterName: ClusterName, notebookServiceAccount: Option[WorkbenchEmail]): Future[Unit] = {
    // Delete the notebook service account key in Google, if present
    val tea = for {
      key <- OptionT(dbRef.inTransaction { _.clusterQuery.getServiceAccountKeyId(googleProject, clusterName) })
      serviceAccountEmail <- OptionT.fromOption[Future](notebookServiceAccount)
      _ <- OptionT.liftF(googleIamDAO.removeServiceAccountKey(dataprocConfig.leoGoogleProject, serviceAccountEmail, key))
    } yield ()

    tea.value.void
  }

  private def removeServiceAccountKey(cluster: Cluster): Future[Unit] = {
    removeServiceAccountKey(cluster.googleProject, cluster.clusterName, cluster.serviceAccountInfo.notebookServiceAccount)
  }

  private def deleteInitBucket(cluster: Cluster): Future[Unit] = {
    // Get the init bucket path for this cluster, then delete the bucket in Google.
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getInitBucket(cluster.googleProject, cluster.clusterName)
    } flatMap {
      case None =>
        logger.warn(s"Could not lookup bucket for cluster ${cluster.projectNameString}: cluster not in db")
        Future.successful(())
      case Some(bucketPath) =>
        deleteInitBucket(bucketPath.bucketName, cluster.googleProject, cluster.clusterName)
    }
  }

  private def deleteInitBucket(initBucketName: GcsBucketName, googleProject: GoogleProject, clusterName: ClusterName): Future[Unit] = {
    googleStorageDAO.deleteBucket(initBucketName, recurse = true) map { _ =>
      logger.info(s"Successfully deleted init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}")
    } recover { case e =>
      logger.error(s"Failed to delete init bucket ${initBucketName.value} for  ${googleProject.value} / ${clusterName.value}", e)
    }
  }

  private def ensureClusterReadyForProxying(cluster: Cluster, ip: IP): Future[Unit] = {
    // Ensure if the cluster's IP has been picked up by the DNS cache and is ready for proxying.
    implicit val timeout: Timeout = Timeout(5 seconds)
    (clusterDnsCache ? ProcessReadyCluster(cluster.copy(hostIp = Some(ip))))
      .mapTo[Either[Throwable, GetClusterResponse]]
      .map {
        case Left(throwable) => throw throwable
        case Right(ClusterReady(_)) => ()
        case Right(_) => throw ClusterNotReadyException(cluster.googleProject, cluster.clusterName)
      }
  }

  private def removeCredentialsFromMetadata(cluster: Cluster): Future[Unit] = {
    cluster.serviceAccountInfo.notebookServiceAccount match {
      // No notebook service account: don't remove creds from metadata! We need them.
      case None => Future.successful(())

      // Remove credentials from instance metadata.
      // We want to ensure that _only_ the notebook service account is used;
      // users should not be able to yank the cluster SA credentials from the metadata server.
      case Some(_) =>
        // TODO https://github.com/DataBiosphere/leonardo/issues/128
        Future.successful(())
    }
  }


  private def removeIamRolesForUser(cluster: Cluster): Future[Unit] = {
    // Remove the Dataproc Worker IAM role for the cluster service account
    cluster.serviceAccountInfo.clusterServiceAccount match {
      case None => Future.successful(())
      case Some(serviceAccountEmail) =>
        // Only remove the Dataproc Worker role if there are no other clusters with the same owner
        // in the DB with CREATING status. This prevents situations where we prematurely yank pet SA
        // roles when the same user is creating multiple clusters.
        dbRef.inTransaction { dataAccess =>
          dataAccess.clusterQuery.countByClusterServiceAccountAndStatus(serviceAccountEmail, ClusterStatus.Creating)
        } flatMap { count =>
          if (count > 0) {
            Future.successful(())
          } else {
            googleIamDAO.removeIamRolesForUser(cluster.googleProject, serviceAccountEmail, Set("roles/dataproc.worker"))
          }
        }
    }
  }

//  private def getMasterIp(cluster: Cluster): Future[Option[IP]] = {
//    val transformed = for {
//      masterKey <- OptionT(gdDAO.getClusterMasterInstance(cluster.googleProject, cluster.clusterName))
//      masterInstance <- OptionT(googleComputeDAO.getInstance(masterKey))
//      masterIp <- OptionT.fromOption[Future](masterInstance.ip)
//    } yield masterIp
//
//    transformed.value
//  }

  private def getErrorDetails(cluster: Cluster, googleState: GoogleState): IO[GoogleState] = {
    if (googleState.clusterStatus == ClusterStatus.Error) {
      gdDAO.getClusterErrorDetails(cluster.operationName).asIO.map(errorDetails => googleState.copy(errorDetails = errorDetails))
    } else IO(googleState)
  }

  private def getMasterIp(googleState: GoogleState): GoogleState = {
    if (googleState.clusterStatus == ClusterStatus.Running) {
      val ip = googleState.instances.find(i => i.dataprocRole == Some(DataprocRole.Master) && i.status == InstanceStatus.Running).flatMap(_.ip)
      googleState.copy(masterIp = ip)
    } else googleState
  }

  private def getClusterInstances(cluster: Cluster): Future[Set[Instance]] = {
    for {
      map <- gdDAO.getClusterInstances(cluster.googleProject, cluster.clusterName)
      instances <- Future.traverse(map) { case (role, instances) =>
        Future.traverse(instances) { instance =>
          googleComputeDAO.getInstance(instance).map(_.map(_.copy(dataprocRole = Some(role))))
        }
      }
    } yield instances.flatten.flatten.toSet
  }

  private def persistInstances(cluster: Cluster, instances: Set[Instance]): Future[Unit] = {
    logger.debug(s"Persisting instances for cluster ${cluster.projectNameString}: ${instances}")
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.upsertInstances(cluster.copy(instances = instances))
    }.void
  }

  private def scheduler = Scheduler[IO](corePoolSize = 5)

  private implicit class FutureToIO[A](f: Future[A]) {
    def asIO: IO[A] = IO.fromFuture(IO(f))
  }

}
