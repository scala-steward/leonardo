package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.io.ByteArrayInputStream
import java.net.URL

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.mock.{FakeGoogleComputeService, MockComputePollOperation}
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, MockGoogleDiskService}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.ContainerRegistry.GCR
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.auth.WhitelistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockDockerDAO, MockSamDAO, MockWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.util.Retry
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext.Implicits.global

class LeonardoServiceSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll
    with TestComponent
    with ScalaFutures
    with OptionValues
    with Retry
    with LazyLogging {

  private var gdDAO: MockGoogleDataprocDAO = _
  private var directoryDAO: MockGoogleDirectoryDAO = _
  private var iamDAO: MockGoogleIamDAO = _
  private var projectDAO: MockGoogleProjectDAO = _
  private var storageDAO: MockGoogleStorageDAO = _
  private var samDao: MockSamDAO = _
  private var bucketHelper: BucketHelper[IO] = _
  private var dataprocInterp: RuntimeAlgebra[IO] = _
  private var gceInterp: RuntimeAlgebra[IO] = _
  private var leo: LeonardoService = _
  private var authProvider: LeoAuthProvider[IO] = _

  val mockPetGoogleDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  before {
    gdDAO = new MockGoogleDataprocDAO
    directoryDAO = new MockGoogleDirectoryDAO()
    iamDAO = new MockGoogleIamDAO
    projectDAO = new MockGoogleProjectDAO
    storageDAO = new MockGoogleStorageDAO
    // Pre-populate the juptyer extension bucket in the mock storage DAO, as it is passed in some requests
    // Value should match userJupyterExtensionConfig variable
    storageDAO.buckets += GcsBucketName("bucket-name") -> Set(
      (GcsObjectName("extension"), new ByteArrayInputStream("foo".getBytes()))
    )

    // Set up the mock directoryDAO to have the Google group used to grant permission to users to pull the custom dataproc image
    directoryDAO
      .createGroup(
        Config.googleGroupsConfig.dataprocImageProjectGroupName,
        Config.googleGroupsConfig.dataprocImageProjectGroupEmail,
        Option(directoryDAO.lockedDownGroupSettings)
      )
      .futureValue

    samDao = serviceAccountProvider.samDao
    authProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)

    val bucketHelperConfig =
      BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig)
    val bucketHelper =
      new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, serviceAccountProvider, blocker)
    val vpcInterp = new VPCInterpreter[IO](Config.vpcInterpreterConfig,
                                           projectDAO,
                                           FakeGoogleComputeService,
                                           new MockComputePollOperation)
    dataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                 bucketHelper,
                                                 vpcInterp,
                                                 gdDAO,
                                                 FakeGoogleComputeService,
                                                 MockGoogleDiskService,
                                                 directoryDAO,
                                                 iamDAO,
                                                 projectDAO,
                                                 MockWelderDAO,
                                                 blocker)
    gceInterp = new GceInterpreter[IO](Config.gceInterpreterConfig,
                                       bucketHelper,
                                       vpcInterp,
                                       FakeGoogleComputeService,
                                       MockGoogleDiskService,
                                       MockWelderDAO,
                                       blocker)
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    leo = new LeonardoService(dataprocConfig,
                              imageConfig,
                              MockWelderDAO,
                              proxyConfig,
                              swaggerConfig,
                              autoFreezeConfig,
                              Config.zombieRuntimeMonitorConfig,
                              welderConfig,
                              mockPetGoogleDAO,
                              authProvider,
                              serviceAccountProvider,
                              bucketHelper,
                              new MockDockerDAO,
                              QueueFactory.makePublisherQueue())
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "LeonardoService" should "create a single node cluster with default machine configs" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeRunSync()

    // check the create response has the correct info
    clusterCreateResponse.clusterName shouldBe name0
    clusterCreateResponse.googleProject shouldBe project
    clusterCreateResponse.serviceAccountInfo shouldEqual clusterServiceAccountFromProject(project).get
    clusterCreateResponse.asyncRuntimeFields shouldBe None
    clusterCreateResponse.auditInfo.creator shouldBe userInfo.userEmail
    clusterCreateResponse.auditInfo.destroyedDate shouldBe None
    clusterCreateResponse.runtimeConfig shouldEqual singleNodeDefaultMachineConfig
    clusterCreateResponse.status shouldBe RuntimeStatus.Creating
    clusterCreateResponse.jupyterExtensionUri shouldBe None
    clusterCreateResponse.jupyterUserScriptUri shouldBe testClusterRequest.jupyterUserScriptUri
    clusterCreateResponse.jupyterStartUserScriptUri shouldBe testClusterRequest.jupyterStartUserScriptUri
    clusterCreateResponse.errors shouldBe List.empty
    clusterCreateResponse.dataprocInstances shouldBe Set.empty
    clusterCreateResponse.userJupyterExtensionConfig shouldBe testClusterRequest.userJupyterExtensionConfig
    clusterCreateResponse.autopauseThreshold shouldBe testClusterRequest.autopauseThreshold.getOrElse(0)
    clusterCreateResponse.defaultClientId shouldBe testClusterRequest.defaultClientId
    clusterCreateResponse.stopAfterCreation shouldBe testClusterRequest.stopAfterCreation.getOrElse(false)
    clusterCreateResponse.clusterImages.map(_.imageType) shouldBe Set(Jupyter, Proxy)
    clusterCreateResponse.scopes shouldBe dataprocConfig.defaultScopes
    clusterCreateResponse.welderEnabled shouldBe false
    clusterCreateResponse.labels.get("tool") shouldBe Some("Jupyter")
    clusterCreateResponse.clusterUrl shouldBe new URL(s"https://leo/proxy/${project.value}/${name0.asString}/jupyter")

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(clusterCreateResponse.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster.get, clusterCreateResponse)

    // check that no state in Google changed
    // TODO
//    computeDAO.firewallRules shouldBe 'empty
    storageDAO.buckets.keySet shouldBe Set(GcsBucketName("bucket-name"))

    // init bucket should not have been persisted to the database
    val dbInitBucketOpt = dbFutureValue(clusterQuery.getInitBucket(project, name0))
    dbInitBucketOpt shouldBe None
  }

  it should "create a cluster with the latest welder from welderRegistry" in isolatedDbTest {
    // create the cluster
    val clusterRequest1 = testClusterRequest.copy(
      runtimeConfig = Some(singleNodeDefaultMachineConfigRequest),
      stopAfterCreation = Some(true),
      welderRegistry = Some(ContainerRegistry.GCR),
      enableWelder = Some(true)
    )
    val clusterRequest2 = testClusterRequest.copy(
      runtimeConfig = Some(singleNodeDefaultMachineConfigRequest),
      stopAfterCreation = Some(true),
      welderRegistry = Some(ContainerRegistry.DockerHub),
      enableWelder = Some(true)
    )
    val clusterRequest3 = testClusterRequest.copy(
      runtimeConfig = Some(singleNodeDefaultMachineConfigRequest),
      stopAfterCreation = Some(true),
      enableWelder = Some(true)
    )

    val clusterResponse1 = leo.createCluster(userInfo, project, name1, clusterRequest1).unsafeToFuture.futureValue
    val clusterResponse2 = leo.createCluster(userInfo, project, name2, clusterRequest2).unsafeToFuture.futureValue
    val clusterResponse3 = leo.createCluster(userInfo, project, name3, clusterRequest3).unsafeToFuture.futureValue

    // check the cluster persisted to the database matches the create response
    val dbCluster1 = dbFutureValue(clusterQuery.getClusterById(clusterResponse1.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster1.get, clusterResponse1)
    val dbCluster2 = dbFutureValue(clusterQuery.getClusterById(clusterResponse2.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster2.get, clusterResponse2)
    val dbCluster3 = dbFutureValue(clusterQuery.getClusterById(clusterResponse3.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster3.get, clusterResponse3)

    // cluster images should contain correct welder images
    clusterResponse1.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(
      imageConfig.welderGcrImage.imageUrl
    )
    clusterResponse2.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(
      imageConfig.welderDockerHubImage.imageUrl
    )
    clusterResponse3.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(
      imageConfig.welderGcrImage.imageUrl
    )
  }

  it should "create a cluster with a client-supplied welder image" in isolatedDbTest {
    val customWelderImage = ContainerImage("my-custom-welder-image-link", GCR)

    // create the cluster
    val clusterRequest = testClusterRequest.copy(
      runtimeConfig = Some(singleNodeDefaultMachineConfigRequest),
      stopAfterCreation = Some(true),
      welderDockerImage = Some(customWelderImage),
      enableWelder = Some(true)
    )

    val clusterResponse = leo.createCluster(userInfo, project, name1, clusterRequest).unsafeToFuture.futureValue

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(clusterResponse.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster.get, clusterResponse)

    // cluster images should contain welder and Jupyter
    clusterResponse.clusterImages.find(_.imageType == Jupyter).map(_.imageUrl) shouldBe Some(
      imageConfig.jupyterImage.imageUrl
    )
    clusterResponse.clusterImages.find(_.imageType == RStudio) shouldBe None
    clusterResponse.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(customWelderImage.imageUrl)
  }

  it should "create a cluster with an rstudio image" in isolatedDbTest {
    val rstudioImage = ContainerImage("some-rstudio-image", GCR)

    // create the cluster
    val clusterRequest = testClusterRequest.copy(toolDockerImage = Some(rstudioImage), enableWelder = Some(true))
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val leoForTest = new LeonardoService(dataprocConfig,
                                         imageConfig,
                                         MockWelderDAO,
                                         proxyConfig,
                                         swaggerConfig,
                                         autoFreezeConfig,
                                         Config.zombieRuntimeMonitorConfig,
                                         welderConfig,
                                         mockPetGoogleDAO,
                                         authProvider,
                                         serviceAccountProvider,
                                         bucketHelper,
                                         new MockDockerDAO(RStudio),
                                         QueueFactory.makePublisherQueue())

    val clusterResponse = leoForTest.createCluster(userInfo, project, name1, clusterRequest).unsafeToFuture.futureValue

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(clusterResponse.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster.get, clusterResponse)

    // cluster images should contain welder and RStudio
    clusterResponse.clusterImages.find(_.imageType == RStudio).map(_.imageUrl) shouldBe Some(rstudioImage.imageUrl)
    clusterResponse.clusterImages.find(_.imageType == Jupyter) shouldBe None
    clusterResponse.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(
      imageConfig.welderGcrImage.imageUrl
    )
    clusterResponse.labels.get("tool") shouldBe Some("RStudio")
    clusterResponse.clusterUrl shouldBe new URL(s"https://leo/proxy/${project.value}/${name1.asString}/rstudio")
  }

  it should "create a single node with default machine config cluster when no machine config is set" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(runtimeConfig = None)

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual dataprocConfig.runtimeConfigDefaults
  }

  it should "create a single node cluster with zero workers explicitly defined in machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          numberOfWorkers = Some(0),
          masterMachineType = Some(dataprocConfig.runtimeConfigDefaults.masterMachineType),
          masterDiskSize = Some(dataprocConfig.runtimeConfigDefaults.masterDiskSize),
          None,
          None,
          None,
          None,
          Map.empty
        )
      )
    )
    val expectedRuntimeConfig = RuntimeConfig.DataprocConfig(
      numberOfWorkers = 0,
      masterMachineType = dataprocConfig.runtimeConfigDefaults.masterMachineType,
      masterDiskSize = dataprocConfig.runtimeConfigDefaults.masterDiskSize,
      None,
      None,
      None,
      None,
      Map.empty
    )

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual expectedRuntimeConfig
  }

  it should "create a single node cluster with master configs defined" in isolatedDbTest {
    val singleNodeDefinedMachineConfigReq =
      RuntimeConfigRequest.DataprocConfig(Some(0),
                                          Some(MachineTypeName("test-master-machine-type2")),
                                          Some(DiskSize(200)),
                                          None,
                                          None,
                                          None,
                                          None,
                                          Map.empty)

    val expectedRuntimeConfig = RuntimeConfig.DataprocConfig(
      numberOfWorkers = 0,
      masterMachineType = MachineTypeName("test-master-machine-type2"),
      masterDiskSize = DiskSize(200),
      workerMachineType = None,
      workerDiskSize = None,
      numberOfWorkerLocalSSDs = None,
      numberOfPreemptibleWorkers = None,
      properties = Map.empty
    )

    val clusterRequestWithMachineConfig =
      testClusterRequest.copy(runtimeConfig = Some(singleNodeDefinedMachineConfigReq))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual expectedRuntimeConfig
  }

  it should "create a single node cluster and override worker configs" in isolatedDbTest {
    // machine config is creating a single node cluster, but has worker configs defined
    val machineConfig = Some(
      RuntimeConfigRequest.DataprocConfig(
        Some(0),
        Some(MachineTypeName("test-master-machine-type3")),
        Some(DiskSize(200)),
        Some(MachineTypeName("test-worker-machine-type")),
        Some(DiskSize(10)),
        Some(3),
        Some(4),
        Map.empty
      )
    )
    val clusterRequestWithMachineConfig = testClusterRequest.copy(runtimeConfig = machineConfig)

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual RuntimeConfig.DataprocConfig(
      0,
      MachineTypeName("test-master-machine-type3"),
      DiskSize(200),
      None,
      None,
      None,
      None,
      Map.empty
    )
  }

  it should "create a standard cluster with 2 workers with default worker configs" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          numberOfWorkers = Some(2),
          masterDiskSize = Some(singleNodeDefaultMachineConfig.masterDiskSize),
          masterMachineType = Some(singleNodeDefaultMachineConfig.masterMachineType),
          workerMachineType = None,
          workerDiskSize = None,
          numberOfWorkerLocalSSDs = None,
          numberOfPreemptibleWorkers = None,
          properties = Map.empty
        )
      )
    )
    val machineConfigResponse = RuntimeConfig.DataprocConfig(
      2,
      singleNodeDefaultMachineConfig.masterMachineType,
      singleNodeDefaultMachineConfig.masterDiskSize,
      singleNodeDefaultMachineConfig.workerMachineType,
      singleNodeDefaultMachineConfig.workerDiskSize,
      singleNodeDefaultMachineConfig.numberOfWorkerLocalSSDs,
      singleNodeDefaultMachineConfig.numberOfPreemptibleWorkers,
      Map.empty
    )

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual machineConfigResponse
  }

  it should "create a standard cluster with 10 workers with defined config" in isolatedDbTest {
    val machineConfig = RuntimeConfigRequest.DataprocConfig(
      Some(10),
      Some(MachineTypeName("test-master-machine-type")),
      Some(DiskSize(200)),
      Some(MachineTypeName("test-worker-machine-type")),
      Some(DiskSize(300)),
      Some(3),
      Some(4),
      Map.empty
    )
    val expectedRuntimeConfig = RuntimeConfig.DataprocConfig(
      numberOfWorkers = 10,
      masterMachineType = MachineTypeName("test-master-machine-type"),
      masterDiskSize = DiskSize(200),
      workerMachineType = Some(MachineTypeName("test-worker-machine-type")),
      workerDiskSize = Some(DiskSize(300)),
      numberOfWorkerLocalSSDs = Some(3),
      numberOfPreemptibleWorkers = Some(4),
      properties = Map.empty
    )
    val clusterRequestWithMachineConfig = testClusterRequest.copy(runtimeConfig = Some(machineConfig))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual expectedRuntimeConfig
  }

  it should "create a standard cluster with 2 workers and override too-small disk sizes with minimum disk size" in isolatedDbTest {
    val machineConfig = RuntimeConfigRequest.DataprocConfig(
      Some(2),
      Some(MachineTypeName("test-master-machine-type")),
      Some(DiskSize(5)),
      Some(MachineTypeName("test-worker-machine-type")),
      Some(DiskSize(5)),
      Some(3),
      Some(4),
      Map.empty
    )
    val clusterRequestWithMachineConfig = testClusterRequest.copy(runtimeConfig = Some(machineConfig))
    val expectedMachineConfig = RuntimeConfig.DataprocConfig(
      2,
      MachineTypeName("test-master-machine-type"),
      DiskSize(10),
      Some(MachineTypeName("test-worker-machine-type")),
      Some(DiskSize(10)),
      Some(3),
      Some(4),
      Map.empty
    )

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual expectedMachineConfig
  }

//  it should "list no clusters" in isolatedDbTest {
//    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue shouldBe 'empty
//    leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).unsafeToFuture.futureValue shouldBe 'empty
//  }
//
//  it should "list all clusters" in isolatedDbTest {
//    // create a couple of clusters
//    val clusterName1 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
//    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).unsafeToFuture.futureValue
//
//    val clusterName2 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
//    val cluster2 = leo
//      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
//      .unsafeToFuture
//      .futureValue
//
//    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
//      LeoLenses.createRuntimeRespToListRuntimeResp.get
//    )
//  }
//
//  it should "error when trying to delete a creating cluster" in isolatedDbTest {
//    // create cluster
//    leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.futureValue
//
//    // should fail to delete because cluster is in Creating status
//    leo
//      .deleteCluster(userInfo, project, name0)
//      .unsafeToFuture
//      .failed
//      .futureValue shouldBe a[RuntimeCannotBeDeletedException]
//  }
//
//  it should "list all active clusters" in isolatedDbTest {
//    // create a couple of clusters
//    val cluster1 = leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue
//
//    val clusterName2 = RuntimeName("test-cluster-2")
//    val cluster2 = leo
//      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
//      .unsafeToFuture
//      .futureValue
//
//    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).unsafeToFuture.futureValue.toSet shouldBe Set(
//      cluster1,
//      cluster2
//    ).map(
//      LeoLenses.createRuntimeRespToListRuntimeResp.get
//    )
//    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
//      LeoLenses.createRuntimeRespToListRuntimeResp.get
//    )
//    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
//      LeoLenses.createRuntimeRespToListRuntimeResp.get
//    )
//
//    val clusterName3 = RuntimeName("test-cluster-3")
//    val cluster3 = leo
//      .createCluster(userInfo, project, clusterName3, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
//      .unsafeToFuture
//      .futureValue
//
//    dbFutureValue(clusterQuery.completeDeletion(cluster3.id, Instant.now))
//
//    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
//      LeoLenses.createRuntimeRespToListRuntimeResp.get
//    )
//    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).unsafeToFuture.futureValue.toSet shouldBe Set(
//      cluster1,
//      cluster2
//    ).map(
//      LeoLenses.createRuntimeRespToListRuntimeResp.get
//    )
//    leo.listClusters(userInfo, Map("includeDeleted" -> "true")).unsafeToFuture.futureValue.toSet.size shouldBe 3
//  }
//
//  it should "list clusters with labels" in isolatedDbTest {
//    // create a couple of clusters
//    val clusterName1 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
//    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).unsafeToFuture.futureValue
//
//    val clusterName2 = RuntimeName(s"test-cluster-2")
//    val cluster2 = leo
//      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
//      .unsafeToFuture
//      .futureValue
//
//    leo.listClusters(userInfo, Map("foo" -> "bar")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2)
//      .map(
//        LeoLenses.createRuntimeRespToListRuntimeResp.get
//      )
//    leo.listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes")).unsafeToFuture.futureValue.toSet shouldBe Set(
//      cluster1
//    ).map(
//      LeoLenses.createRuntimeRespToListRuntimeResp.get
//    )
//    leo
//      .listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes", "vcf" -> "no"))
//      .unsafeToFuture
//      .futureValue
//      .toSet shouldBe Set(
//      cluster1
//    ).map(LeoLenses.createRuntimeRespToListRuntimeResp.get)
//    leo.listClusters(userInfo, Map("a" -> "b")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster2).map(
//      LeoLenses.createRuntimeRespToListRuntimeResp.get
//    )
//    leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).unsafeToFuture.futureValue.toSet shouldBe Set.empty
//    leo
//      .listClusters(userInfo, Map("A" -> "B"))
//      .unsafeToFuture
//      .futureValue
//      .toSet shouldBe Set(cluster2).map(
//      LeoLenses.createRuntimeRespToListRuntimeResp.get
//    ) // labels are not case sensitive because MySQL
//    //Assert that extensions were added as labels as well
//    leo
//      .listClusters(userInfo, Map("abc" -> "def", "pqr" -> "pqr", "xyz" -> "xyz"))
//      .unsafeToFuture
//      .futureValue
//      .toSet shouldBe Set(
//      cluster1,
//      cluster2
//    ).map(LeoLenses.createRuntimeRespToListRuntimeResp.get)
//  }

  it should "throw IllegalLabelKeyException when using a forbidden label" in isolatedDbTest {
    // cluster should not be allowed to have a label with key of "includeDeleted"
    val modifiedTestClusterRequest = testClusterRequest.copy(labels = Map("includeDeleted" -> "val"))
    val includeDeletedResponse =
      leo.createCluster(userInfo, project, name0, modifiedTestClusterRequest).unsafeToFuture.failed.futureValue

    includeDeletedResponse shouldBe a[IllegalLabelKeyException]
  }

  it should "calculate autopause threshold properly" in {
    LeonardoService.calculateAutopauseThreshold(None, None, autoFreezeConfig) shouldBe autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
    LeonardoService.calculateAutopauseThreshold(Some(false), None, autoFreezeConfig) shouldBe autoPauseOffValue
    LeonardoService.calculateAutopauseThreshold(Some(true), None, autoFreezeConfig) shouldBe autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
    LeonardoService.calculateAutopauseThreshold(Some(true), Some(30), autoFreezeConfig) shouldBe 30
  }

  it should "extract labels properly" in {
    val input1 = Map("_labels" -> "foo=bar,baz=biz")
    LeonardoService.processLabelMap(input1) shouldBe (Right(Map("foo" -> "bar", "baz" -> "biz")))

    val failureInput = Map("_labels" -> "foo=bar,,baz=biz")
    LeonardoService.processLabelMap(failureInput).isLeft shouldBe (true)

    val duplicateLabel = Map("_labels" -> "foo=bar,foo=biz")
    LeonardoService.processLabelMap(duplicateLabel) shouldBe (Right(Map("foo" -> "biz")))
  }

  private def withLeoPublisher(
    publisherQueue: InspectableQueue[IO, LeoPubsubMessage]
  )(validations: IO[Assertion]): IO[Assertion] = {
    val leoPublisher = new LeoPublisher[IO](publisherQueue, FakeGooglePublisher)
    withInfiniteStream(leoPublisher.process, validations)
  }

  private def makeLeo(
    queue: InspectableQueue[IO, LeoPubsubMessage] = QueueFactory.makePublisherQueue()
  )(implicit system: ActorSystem): LeonardoService =
    new LeonardoService(dataprocConfig,
                        imageConfig,
                        MockWelderDAO,
                        proxyConfig,
                        swaggerConfig,
                        autoFreezeConfig,
                        Config.zombieRuntimeMonitorConfig,
                        welderConfig,
                        mockPetGoogleDAO,
                        authProvider,
                        serviceAccountProvider,
                        bucketHelper,
                        new MockDockerDAO,
                        queue)
}
