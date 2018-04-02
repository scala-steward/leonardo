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
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, MonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.{ClusterReady, GetClusterResponse, ProcessReadyCluster}
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterError, LeoAuthProvider, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.{Deleted, Deleting}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorActor.{ClusterMonitorMessage, ShutdownActor}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterDeleted
import org.broadinstitute.dsde.workbench.leonardo.service.ClusterNotReadyException
import slick.dbio.DBIOAction

import scala.collection.immutable.Set
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Created by rtitle on 3/29/18.
  */
class ClusterMonitorHelper(dbRef: DbReference, gdDAO: GoogleDataprocDAO, googleComputeDAO: GoogleComputeDAO, googleStorageDAO: GoogleStorageDAO, googleIamDAO: GoogleIamDAO, clusterDnsCache: ActorRef, dataprocConfig: DataprocConfig, monitorConfig: MonitorConfig, authProvider: LeoAuthProvider)(implicit executionContext: ExecutionContext) extends LazyLogging {

  case class GoogleState(clusterStatus: ClusterStatus, instances: Set[Instance], masterIp: Option[IP], errorDetails: Option[ClusterErrorDetails])

  def monitorCreation(cluster: Cluster): IO[Boolean] = {
    val beginLog = Stream.eval_[IO, Unit] { IO(logger.info(s"Monitoring cluster ${cluster.projectNameString} for initialization")) }

    val pollResult = clusterStatusAndInstances(cluster)
      .evalMap { g => persistInstances(cluster, g.instances).asIO.map(_ => g) }
      .evalMap { g =>
        if (g.clusterStatus == ClusterStatus.Error) {
          gdDAO.getClusterErrorDetails(cluster.operationName).asIO.map(errorDetails => g.copy(errorDetails = errorDetails))
        } else IO(g)
      }
      .evalMap { g =>
        if (g.clusterStatus == ClusterStatus.Running) {
          getMasterIp(cluster).asIO.map(ip => g.copy(masterIp = ip))
        } else IO(g)
      }

      .takeThrough { g =>
        g.clusterStatus != ClusterStatus.Running || g.clusterStatus != ClusterStatus.Error ||
          (g.clusterStatus == ClusterStatus.Running && g.masterIp.isEmpty) ||
          (g.clusterStatus == ClusterStatus.Error && g.errorDetails.isEmpty)
      }

      .lastOr(sys.error("[fs2] impossible: empty stream in retry"))

    (beginLog ++ pollResult).flatMap {
      case GoogleState(ClusterStatus.Running, instances, Some(ip), _) =>
        Stream.eval(handleReadyCluster(cluster, ip, instances).asIO.map(_ => false))

      case GoogleState(ClusterStatus.Error, instances, _, Some(errorDetails)) =>
        Stream.eval(handleFailedCluster(cluster, errorDetails, instances).asIO)

      case s =>
        Stream.eval(IO.raiseError(new Exception(s"stopping monitoring on cluster ${cluster.projectNameString} with state $s")))

    }.compile.toList.map(_.last)
  }

  def monitorDeletion(cluster: Cluster): IO[Unit] = {
    val beginLog = Stream.eval_[IO, Unit] { IO(logger.info(s"Monitoring cluster ${cluster.projectNameString} for deletion")) }

    val pollResult = clusterStatusAndInstances(cluster)
      .takeThrough { g => g.clusterStatus != ClusterStatus.Deleted }
      .lastOr(sys.error("[fs2] impossible: empty stream in retry"))

    (beginLog ++ pollResult).flatMap {
      case GoogleState(ClusterStatus.Deleted, _, _, _) =>
        Stream.eval(handleDeletedCluster(cluster).asIO)

      case s =>
        Stream.eval(IO.raiseError(new Exception(s"stopping monitoring on cluster ${cluster.projectNameString} with state $s")))

    }.compile.toList.map(_.last)
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

  private def shouldRecreateCluster(code: Int, message: Option[String]): Boolean = {
    // TODO: potentially add more checks here as we learn which errors are recoverable
    monitorConfig.recreateCluster && (code == Code.UNKNOWN.value)
  }

  private def removeServiceAccountKey(cluster: Cluster): Future[Unit] = {
    // Delete the notebook service account key in Google, if present
    val tea = for {
      key <- OptionT(dbRef.inTransaction { _.clusterQuery.getServiceAccountKeyId(cluster.googleProject, cluster.clusterName) })
      serviceAccountEmail <- OptionT.fromOption[Future](cluster.serviceAccountInfo.notebookServiceAccount)
      _ <- OptionT.liftF(googleIamDAO.removeServiceAccountKey(dataprocConfig.leoGoogleProject, serviceAccountEmail, key))
    } yield ()

    tea.value.void
  }

  private def deleteInitBucket(cluster: Cluster): Future[Unit] = {
    // Get the init bucket path for this cluster, then delete the bucket in Google.
    dbRef.inTransaction { dataAccess =>
      dataAccess.clusterQuery.getInitBucket(cluster.googleProject, cluster.clusterName)
    } flatMap {
      case None => Future.successful( logger.warn(s"Could not lookup bucket for cluster ${cluster.projectNameString}: cluster not in db") )
      case Some(bucketPath) =>
        googleStorageDAO.deleteBucket(bucketPath.bucketName, recurse = true) map { _ =>
          logger.debug(s"Deleted init bucket $bucketPath for cluster ${cluster.googleProject}/${cluster.clusterName}")
        }
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

  private def getMasterIp(cluster: Cluster): Future[Option[IP]] = {
    val transformed = for {
      masterKey <- OptionT(gdDAO.getClusterMasterInstance(cluster.googleProject, cluster.clusterName))
      masterInstance <- OptionT(googleComputeDAO.getInstance(masterKey))
      masterIp <- OptionT.fromOption[Future](masterInstance.ip)
    } yield masterIp

    transformed.value
  }

  private def getMasterIp(instances: Set[Instance]): Option[IP] = {
    instances.find(_.dataprocRole == Some(DataprocRole.Master)).flatMap(_.ip)
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

  def scheduler = Scheduler[IO](corePoolSize = 5)

  private implicit class FutureToIO[A](f: Future[A]) {
    def asIO: IO[A] = IO.fromFuture(IO(f))
  }

}
