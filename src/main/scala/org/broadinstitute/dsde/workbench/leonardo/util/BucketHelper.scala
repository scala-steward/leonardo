package org.broadinstitute.dsde.workbench.leonardo.util

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import cats.data
import cats.data.{NonEmptyList, OptionT}
import cats.effect.IO
import cats.implicits._
import com.google.cloud.Identity
import com.typesafe.scalalogging.LazyLogging
import fs2._
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, ServiceAccountInfo, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsEntityTypes.{Group, User}
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.{GcsRole, Owner, Reader}
import org.broadinstitute.dsde.workbench.model.google.ProjectTeamTypes.{Editors, Owners, Viewers}
import org.broadinstitute.dsde.workbench.model.google.{EmailGcsEntity, GcsBucketName, GcsEntity, GoogleProject, ProjectGcsEntity, ProjectNumber}
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse, GoogleStorageService, StorageRole}
import org.broadinstitute.dsde.workbench.util.Retry

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

//case class NoGoogleProjectNumberException(googleProject: GoogleProject)
//  extends LeoException(
//    s"Project number could not be found for Google project $googleProject",
//    StatusCodes.NotFound)

/**
  * Created by rtitle on 2/7/18.
  */
class BucketHelper(dataprocConfig: DataprocConfig,
                   gdDAO: GoogleDataprocDAO,
                   googleComputeDAO: GoogleComputeDAO,
                   googleStorageDAO: GoogleStorageDAO,
                   google2StorageDAO: GoogleStorageService[IO],
                   serviceAccountProvider: ServiceAccountProvider)
                  (implicit val executionContext: ExecutionContext, val system: ActorSystem)
  extends LazyLogging with Retry {

  /**
    * Creates the dataproc init bucket and sets the necessary ACLs.
    */
  def createInitBucket(googleProject: GoogleProject, bucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Stream[IO, Unit] = {
    for {
      // The init bucket is created in the cluster's project.
      // Leo service account -> Owner
      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Reader
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)
      leoEntity = Identity.user(serviceAccountProvider.getLeoServiceAccountAndKey._1.value)

      readerAcl = NonEmptyList.fromList(bucketSAs).map(readers => Map(StorageRole.ObjectViewer -> readers)).getOrElse(Map.empty)
      ownerAcl = Map(StorageRole.ObjectAdmin -> NonEmptyList.one(leoEntity))

      // When we receive a lot of simultaneous cluster creation requests in the same project,
      // we hit Google bucket creation/deletion request quota of one per approx. two seconds.
      // Therefore, we are adding a second layer of retries on top of the one existing within
      // the googleStorageDAO.createBucket method
      _ <- google2StorageDAO.createBucket(googleProject, bucketName)
      _ <- google2StorageDAO.setIamPolicy(bucketName, readerAcl ++ ownerAcl)


//      _ <- retryUntilSuccessOrTimeout(failureLogMessage = s"Init bucket creation failed for Google project '$googleProject'")(30 seconds, 5 minutes) { () =>
//        googleStorageDAO.createBucket(googleProject, bucketName, bucketSAs, List(leoEntity))
//      }
    } yield ()
  }

  /**
    * Creates the dataproc staging bucket and sets the necessary ACLs.
    */
  def createStagingBucket(userEmail: WorkbenchEmail, googleProject: GoogleProject, bucketName: GcsBucketName, serviceAccountInfo: ServiceAccountInfo): Stream[IO, Unit] = {
    for {
      // The staging bucket is created in the cluster's project.
      // Leo service account -> Owner
      // Available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Owner
      // Additional readers (users and groups) are specified by the service account provider.
      // Convenience values for projects (to address https://github.com/DataBiosphere/leonardo/issues/317)
      //    viewers-<project number> -> Reader
      //    editors-<project number> -> Owner
      //    owners-<project number> -> Owner
      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)
      leoEntity = Identity.user(serviceAccountProvider.getLeoServiceAccountAndKey._1.value)

      providerReaders <- Stream.eval(IO.fromFuture(IO(serviceAccountProvider.listUsersStagingBucketReaders(userEmail)))).map(_.map(email => Identity.user(email.value)))
      providerGroups <- Stream.eval(IO.fromFuture(IO(serviceAccountProvider.listGroupsStagingBucketReaders(userEmail)))).map(_.map(email => Identity.group(email.value)))

      readerAcl = NonEmptyList.fromList(providerReaders ++ providerGroups).map(readers => Map(StorageRole.ObjectViewer -> readers)).getOrElse(Map.empty)
      ownerAcl = Map(StorageRole.ObjectAdmin ->  NonEmptyList(leoEntity, bucketSAs))

      // When we receive a lot of simultaneous cluster creation requests in the same project,
      // we hit Google bucket creation/deletion request quota of one per approx. two seconds.
      // Therefore, we are adding a second layer of retries on top of the one existing within
      // the googleStorageDAO.createBucket method
      _ <- google2StorageDAO.createBucket(googleProject, bucketName)
      _ <- google2StorageDAO.setIamPolicy(bucketName, readerAcl ++ ownerAcl)

//      _ <- retryUntilSuccessOrTimeout(failureLogMessage = s"Staging bucket creation failed for Google project '$googleProject'")(30 seconds, 5 minutes) { () =>
//        googleStorageDAO.createBucket(googleProject, bucketName, readers, owners)
//      }
    } yield ()
  }

  def storeObject(bucketName: GcsBucketName, objectName: GcsBlobName, objectContents: Array[Byte], objectType: String): IO[Unit] = {
    google2StorageDAO.storeObject(bucketName, objectName, objectContents, objectType)
  }

  def deleteInitBucket(initBucketName: GcsBucketName): IO[Unit] = {
    // TODO: implement deleteBucket in google2
    IO.fromFuture(IO(googleStorageDAO.deleteBucket(initBucketName, recurse = true)))
  }

  /**
    * Sets ACLs on an existing user bucket so that it can be accessed in a dataproc cluster.
    */
//  def updateUserBucket(bucketName: GcsBucketName, googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): Stream[IO, Unit] = {
//    for {
//      // available service accounts ((cluster or default SA) and notebook SA, if they exist) -> Reader
//      bucketSAs <- getBucketSAs(googleProject, serviceAccountInfo)
//
//      _ <- setBucketAcls(bucketName, bucketSAs, List.empty)
//    } yield ()
//  }

//  private def getConvenienceEntities(googleProject: GoogleProject,
//                                     googleProjectNumberOpt: Option[Long]): Future[(GcsEntity, GcsEntity, GcsEntity)] = {
//
//    def createEntities(projectNumber: Long): (GcsEntity, GcsEntity, GcsEntity) = {
//      val projectViewers = ProjectGcsEntity(Viewers, ProjectNumber(projectNumber.toString))
//      val projectEditors = ProjectGcsEntity(Editors, ProjectNumber(projectNumber.toString))
//      val projectOwners = ProjectGcsEntity(Owners, ProjectNumber(projectNumber.toString))
//
//      (projectViewers, projectEditors, projectOwners)
//    }
//
//    googleProjectNumberOpt match {
//      case Some(projectNumber) => Future(createEntities(projectNumber))
//      case _ => Future.failed(NoGoogleProjectNumberException(googleProject))
//    }
//  }

//  private def setBucketAcls(bucketName: GcsBucketName, readers: List[GcsEntity], owners: List[GcsEntity]): Future[Unit] = {
//    def setBucketAndDefaultAcls(entity: GcsEntity, role: GcsRole) = {
//      for {
//        _ <- googleStorageDAO.setBucketAccessControl(bucketName, entity, role)
//        _ <- googleStorageDAO.setDefaultObjectAccessControl(bucketName, entity, role)
//      } yield ()
//    }
//
//    def flatMapList(entities: List[GcsEntity], role: GcsRole): Future[Unit] = {
//      entities match {
//        case Nil => Future.unit
//        case head :: tail =>
//          setBucketAndDefaultAcls(head, role).flatMap(_ => flatMapList(tail, role))
//      }
//    }
//
//    for {
//      _ <- flatMapList(readers, Reader)
//      _ <- flatMapList(owners, Owner)
//    } yield ()
//  }

  private def getBucketSAs(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): IO[List[Identity]] = {
    // cluster SA orElse compute engine default SA
    val clusterOrComputeDefault = OptionT.fromOption[IO](serviceAccountInfo.clusterServiceAccount) orElse
      OptionT(IO.fromFuture(IO(googleComputeDAO.getComputeEngineDefaultServiceAccount(googleProject))))

    // List(cluster or default SA, notebook SA) if they exist
    clusterOrComputeDefault.value.map { clusterOrDefaultSAOpt =>
      List(clusterOrDefaultSAOpt, serviceAccountInfo.notebookServiceAccount).flatten.map(email => Identity.user(email.value))
    }
  }

//  private def getBucketSAs(googleProject: GoogleProject, serviceAccountInfo: ServiceAccountInfo): Future[List[GcsEntity]] = {
//    // cluster SA orElse compute engine default SA
//    val clusterOrComputeDefault = OptionT.fromOption[Future](serviceAccountInfo.clusterServiceAccount) orElse OptionT(googleComputeDAO.getComputeEngineDefaultServiceAccount(googleProject))
//
//    // List(cluster or default SA, notebook SA) if they exist
//    clusterOrComputeDefault.value.map { clusterOrDefaultSAOpt =>
//      List(clusterOrDefaultSAOpt, serviceAccountInfo.notebookServiceAccount).flatten.map(userEntity)
//    }
//  }

//  private def userEntity(email: WorkbenchEmail) = Identity.user(email)//EmailGcsEntity(User, email)
//  private def groupEntity(email: WorkbenchEmail) = EmailGcsEntity(Group, email)
}
