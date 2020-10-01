package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import com.google.protobuf.Timestamp
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.PodStatus
import org.broadinstitute.dsde.workbench.google2.mock.{
  FakeGoogleComputeService,
  MockComputePollOperation,
  MockGKEService
}
import org.broadinstitute.dsde.workbench.google2.{ComputePollOperation, MockGoogleDiskService}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockGalaxyDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsp.mocks.MockHelm
import org.scalatest.concurrent._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

abstract class LeoPubsubMessageSubscriberSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with TestComponent
    with Matchers
    with MockitoSugar
    with Eventually
    with LeonardoTestSuite {

  val mockWelderDAO = mock[WelderDAO[IO]]
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO() {
    override def isGroupMember(groupEmail: WorkbenchEmail,
                               memberEmail: WorkbenchEmail): scala.concurrent.Future[Boolean] =
      scala.concurrent.Future.successful(true)
  }
  val gdDAO = new MockGoogleDataprocDAO
  val storageDAO = new MockGoogleStorageDAO
  // Kubernetes doesn't actually create a new Service Account when calling googleIamDAO
  val iamDAOKubernetes = new MockGoogleIamDAO {
    override def addIamPolicyBindingOnServiceAccount(serviceAccountProject: GoogleProject,
                                                     serviceAccountEmail: WorkbenchEmail,
                                                     memberEmail: WorkbenchEmail,
                                                     rolesToAdd: Set[String]): Future[Unit] = Future.successful(())
  }
  val iamDAO = new MockGoogleIamDAO
  val projectDAO = new MockGoogleProjectDAO
  val authProvider = mock[LeoAuthProvider[IO]]
  val currentTime = Instant.now
  val timestamp = Timestamp.newBuilder().setSeconds(now.toSeconds).build()

  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, serviceAccountProvider, blocker)

  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig,
                           projectDAO,
                           FakeGoogleComputeService,
                           new MockComputePollOperation)

  val gkeInterp =
    new GKEInterpreter[IO](Config.gkeInterpConfig,
                           vpcInterp,
                           MockGKEService,
                           new MockKubernetesService(PodStatus.Succeeded),
                           MockHelm,
                           MockGalaxyDAO,
                           credentials,
                           iamDAOKubernetes,
                           blocker)

  val dataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                   bucketHelper,
                                                   vpcInterp,
                                                   gdDAO,
                                                   FakeGoogleComputeService,
                                                   MockGoogleDiskService,
                                                   mockGoogleDirectoryDAO,
                                                   iamDAO,
                                                   projectDAO,
                                                   mockWelderDAO,
                                                   blocker)
  val gceInterp = new GceInterpreter[IO](Config.gceInterpreterConfig,
                                         bucketHelper,
                                         vpcInterp,
                                         FakeGoogleComputeService,
                                         MockGoogleDiskService,
                                         mockWelderDAO,
                                         blocker)

  implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)

  val runningCluster = makeCluster(1).copy(
    serviceAccount = serviceAccount,
    asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(hostIp = None)),
    status = RuntimeStatus.Running,
    dataprocInstances = Set(masterInstance, workerInstance1, workerInstance2)
  )

  val stoppedCluster = makeCluster(2).copy(
    serviceAccount = serviceAccount,
    asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(hostIp = None)),
    status = RuntimeStatus.Stopped
  )

  def makeLeoSubscriber(runtimeMonitor: RuntimeMonitor[IO, CloudService] = MockRuntimeMonitor,
                        asyncTaskQueue: InspectableQueue[IO, Task[IO]] =
                          InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync,
                        computePollOperation: ComputePollOperation[IO] = new MockComputePollOperation,
                        gkeInterp: GKEInterpreter[IO] = gkeInterp) = {
    val googleSubscriber = new FakeGoogleSubcriber[LeoPubsubMessage]

    implicit val monitor: RuntimeMonitor[IO, CloudService] = runtimeMonitor

    new LeoPubsubMessageSubscriber[IO](
      LeoPubsubMessageSubscriberConfig(1,
                                       30 seconds,
                                       Config.leoPubsubMessageSubscriberConfig.persistentDiskMonitorConfig),
      googleSubscriber,
      asyncTaskQueue,
      MockGoogleDiskService,
      computePollOperation,
      MockAuthProvider,
      gkeInterp,
      org.broadinstitute.dsde.workbench.errorReporting.FakeErrorReporting
    )
  }
}
