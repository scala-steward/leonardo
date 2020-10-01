package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.container.v1
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.PodStatus
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.google2.mock.MockGKEService
import org.broadinstitute.dsde.workbench.google2.{Event, GKEModels, KubernetesModels}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{
  makeApp,
  makeKubeCluster,
  makeNodepool,
  makeService
}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGalaxyDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.{IP, TraceId}
import org.broadinstitute.dsp.mocks.MockHelm
import org.mockito.Mockito.{verify, _}

import scala.concurrent.ExecutionContext.Implicits.global

class LeoPubsubAppMessageSpec extends LeoPubsubMessageSubscriberSpec {

  "CreateAppMessage" should "handle create app message with a create cluster" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      getCluster.nodepools.size shouldBe 2
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors shouldBe List()
      getApp.app.status shouldBe AppStatus.Running
      getApp.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
      getApp.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.cluster.asyncFields shouldBe Some(
        KubernetesClusterAsyncFields(IP("1.2.3.4"),
                                     IP("0.0.0.0"),
                                     NetworkFields(Config.vpcConfig.networkName,
                                                   Config.vpcConfig.subnetworkName,
                                                   Config.vpcConfig.subnetworkIpRange))
      )
      getDisk.status shouldBe DiskStatus.Ready
    }

    val res = for {
      tr <- traceId.ask
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        Some(disk.id),
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "create an app with no disk" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val savedApp1 = makeApp(1, savedNodepool1.id).save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors shouldBe List()
      getApp.cluster.asyncFields shouldBe Some(
        KubernetesClusterAsyncFields(IP("1.2.3.4"),
                                     IP("0.0.0.0"),
                                     NetworkFields(Config.vpcConfig.networkName,
                                                   Config.vpcConfig.subnetworkName,
                                                   Config.vpcConfig.subnetworkIpRange))
      )
      getApp.app.appResources.disk shouldBe None
      getApp.app.status shouldBe AppStatus.Running
      getApp.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
    }

    val res = for {
      tr <- traceId.ask
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "be able to create an multiple apps in a cluster" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedNodepool2 = makeNodepool(2, savedCluster1.id).save()

    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val savedApp2 = makeApp(2, savedNodepool1.id).save()

    val assertions = for {
      getAppOpt1 <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getAppOpt2 <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp2.appName)
        .transaction
      getApp1 = getAppOpt1.get
      getApp2 = getAppOpt2.get
    } yield {
      getApp1.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp2.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp1.nodepool.status shouldBe NodepoolStatus.Running
      getApp2.nodepool.status shouldBe NodepoolStatus.Running
      getApp1.app.errors shouldBe List()
      getApp1.app.status shouldBe AppStatus.Running
      getApp1.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
      getApp1.cluster.asyncFields shouldBe Some(
        KubernetesClusterAsyncFields(IP("1.2.3.4"),
                                     IP("0.0.0.0"),
                                     NetworkFields(Config.vpcConfig.networkName,
                                                   Config.vpcConfig.subnetworkName,
                                                   Config.vpcConfig.subnetworkIpRange))
      )
      getApp2.app.errors shouldBe List()
      getApp2.app.status shouldBe AppStatus.Running
      getApp2.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
    }

    val res = for {
      tr <- traceId.ask
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg1 = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        Some(tr)
      )
      msg2 = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateNodepool(savedNodepool2.id)),
        savedApp2.id,
        savedApp2.appName,
        None,
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg1)
      _ <- leoSubscriber.handleCreateAppMessage(msg2)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  //handle an error in createCluster
  it should "error on create if cluster doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Cluster)
      getApp.app.status shouldBe AppStatus.Error
      //we shouldn't see an error status here because the cluster we passed doesn't exist
      getApp.cluster.status shouldBe KubernetesClusterStatus.Unspecified
    }

    val res = for {
      tr <- traceId.ask
      msg = CreateAppMessage(
        project,
        Some(
          ClusterNodepoolAction.CreateClusterAndNodepool(KubernetesClusterLeoId(-1),
                                                         NodepoolLeoId(-1),
                                                         NodepoolLeoId(-1))
        ),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()
    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  //handle an error in createNodepool
  //update error table and status
  it should "error on create if default nodepool doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id, includeDeletedClusterApps = true)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Cluster)
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, NodepoolLeoId(-1), NodepoolLeoId(-2))),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  it should "error on create if user nodepool doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id, includeDeletedClusterApps = true)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Cluster)
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, NodepoolLeoId(-2))),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "error on create if app doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.App)
    }

    val res = for {
      tr <- traceId.ask
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateNodepool(savedNodepool1.id)),
        savedApp1.id,
        AppName("fakeapp"),
        None,
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "handle error in createApp if createDisk is specified with no disk" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Disk)
    }

    val res = for {
      tr <- traceId.ask
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateNodepool(savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        Some(DiskId(-1)),
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "be able to create app with a pre-created nodepool" in isolatedDbTest {
    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      getCluster.nodepools.size shouldBe 2
      //we shouldn't update the default nodepool status here, its already created
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Unspecified
      getApp.app.errors shouldBe List()
      getApp.app.status shouldBe AppStatus.Running
      getApp.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
      getApp.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getDisk.status shouldBe DiskStatus.Ready
    }
    val res = for {
      tr <- traceId.ask
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        None,
        savedApp1.id,
        savedApp1.appName,
        Some(disk.id),
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  it should "clean-up google resources on error" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    //using a var here to simplify test code
    //we could use mockito for this functionality, but it would be overly complicated since we wish to override other functionality of the mock as well
    var deleteCalled = false

    val mockKubernetesService = new MockKubernetesService(PodStatus.Succeeded) {
      override def createServiceAccount(
        clusterId: GKEModels.KubernetesClusterId,
        serviceAccount: KubernetesModels.KubernetesServiceAccount,
        namespaceName: KubernetesModels.KubernetesNamespace
      )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
        IO.raiseError(new Exception("this is an intentional test exception"))

      override def deleteNamespace(
        clusterId: GKEModels.KubernetesClusterId,
        namespace: KubernetesModels.KubernetesNamespace
      )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
        IO {
          deleteCalled = true
        }
    }

    val gkeInterp =
      new GKEInterpreter[IO](Config.gkeInterpConfig,
                             vpcInterp,
                             MockGKEService,
                             mockKubernetesService,
                             MockHelm,
                             MockGalaxyDAO,
                             credentials,
                             iamDAOKubernetes,
                             blocker)

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      //only the default should be left, the other has been deleted
      getCluster.nodepools.size shouldBe 1
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getApp.app.auditInfo.destroyedDate shouldBe None
      getDisk.status shouldBe DiskStatus.Deleted
      deleteCalled shouldBe true
    }

    val res = for {
      tr <- traceId.ask
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        Some(disk.id),
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, gkeInterp = gkeInterp)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions, maxRetry = 50)
    } yield ()

    res.unsafeRunSync()
  }

  it should "error if nodepools in batch create nodepool message are not in db" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedCluster2 = makeKubeCluster(2).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Precreating).save()
    val savedNodepool2 = makeNodepool(3, savedCluster1.id).copy(status = NodepoolStatus.Precreating).save()
    val savedNodepool3 = makeNodepool(4, savedCluster2.id).copy(status = NodepoolStatus.Unspecified).save()

    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getMinimalCluster <- kubernetesClusterQuery.getMinimalActiveClusterByName(savedCluster1.googleProject).transaction
    } yield {
      getMinimalCluster.get.status shouldBe KubernetesClusterStatus.Error
      getMinimalCluster.get.nodepools.size shouldBe 3
      getMinimalCluster.get.nodepools.filter(_.isDefault).size shouldBe 1
      getMinimalCluster.get.nodepools.filterNot(_.isDefault).size shouldBe 2
      getMinimalCluster.get.nodepools.filterNot(_.isDefault).map(_.status).distinct.size shouldBe 1
      //we should not have updated the status here, since the nodepools given were faulty
      getMinimalCluster.get.nodepools
        .filterNot(_.isDefault)
        .map(_.status)
        .distinct
        .head shouldBe NodepoolStatus.Precreating
    }

    val res = for {
      tr <- traceId.ask
      msg = BatchNodepoolCreateMessage(savedCluster1.id,
                                       List(savedNodepool1.id, savedNodepool2.id, savedNodepool3.id),
                                       savedCluster1.googleProject,
                                       Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  it should "not delete a disk that already existing on error" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val mockKubernetesService = new MockKubernetesService(PodStatus.Succeeded) {
      override def createServiceAccount(
        clusterId: GKEModels.KubernetesClusterId,
        serviceAccount: KubernetesModels.KubernetesServiceAccount,
        namespaceName: KubernetesModels.KubernetesNamespace
      )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
        IO.raiseError(new Exception("this is an intentional test exception"))
    }

    val gkeInterp =
      new GKEInterpreter[IO](Config.gkeInterpConfig,
                             vpcInterp,
                             MockGKEService,
                             mockKubernetesService,
                             MockHelm,
                             MockGalaxyDAO,
                             credentials,
                             iamDAOKubernetes,
                             blocker)

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      //only the default should be left, the other has been deleted
      getCluster.nodepools.size shouldBe 1
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getDisk.status shouldBe disk.status
    }

    val res = for {
      tr <- traceId.ask
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, gkeInterp = gkeInterp)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions, maxRetry = 50)
    } yield ()

    res.unsafeRunSync()
  }

  it should "delete a cluster and put that app in error status on cluster error" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val mockGKEService = new MockGKEService {
      override def createCluster(request: GKEModels.KubernetesCreateClusterRequest)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[com.google.api.services.container.model.Operation] = IO.raiseError(new Exception("test exception"))
    }

    val gkeInterp =
      new GKEInterpreter[IO](Config.gkeInterpConfig,
                             vpcInterp,
                             mockGKEService,
                             new MockKubernetesService(PodStatus.Succeeded),
                             MockHelm,
                             MockGalaxyDAO,
                             credentials,
                             iamDAO,
                             blocker)

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id, true).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id, includeDeletedClusterApps = true)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Deleted
      getCluster.nodepools.map(_.status).distinct shouldBe List(NodepoolStatus.Deleted)
      getApp.app.status shouldBe AppStatus.Error
      getApp.app.errors.size shouldBe 1
    }

    val res = for {
      tr <- traceId.ask
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, gkeInterp = gkeInterp)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  it should "be idempotent" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      getCluster.nodepools.size shouldBe 2
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors shouldBe List()
      getApp.app.status shouldBe AppStatus.Running
      getApp.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
      getApp.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.cluster.asyncFields shouldBe Some(
        KubernetesClusterAsyncFields(IP("1.2.3.4"),
                                     IP("0.0.0.0"),
                                     NetworkFields(Config.vpcConfig.networkName,
                                                   Config.vpcConfig.subnetworkName,
                                                   Config.vpcConfig.subnetworkIpRange))
      )
      getDisk.status shouldBe DiskStatus.Ready
    }

    val res = for {
      tr <- traceId.ask
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        Some(disk.id),
        Map.empty,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      // send message twice
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  "DeleteAppMessage" should "delete app without disk" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  //delete app and not delete disk when specified
  //update app status and disk id
  it should "delete app and not delete disk when specified" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.app.appResources.disk shouldBe None
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getDisk.status shouldBe DiskStatus.Ready
    }

    val res = for {
      tr <- traceId.ask
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "delete app and delete disk when specified" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).copy(status = DiskStatus.Deleting).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getApp.app.errors shouldBe List()
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.app.appResources.disk shouldBe None
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getDisk.status shouldBe DiskStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             Some(disk.id),
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "handle an error in delete app" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val mockKubernetesService = new MockKubernetesService(PodStatus.Failed) {
      override def deleteNamespace(
        clusterId: GKEModels.KubernetesClusterId,
        namespace: KubernetesModels.KubernetesNamespace
      )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.raiseError(new Exception("test error"))
    }
    val gkeInterp =
      new GKEInterpreter[IO](Config.gkeInterpConfig,
                             vpcInterp,
                             MockGKEService,
                             mockKubernetesService,
                             MockHelm,
                             MockGalaxyDAO,
                             credentials,
                             iamDAOKubernetes,
                             blocker)

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getApp.app.errors.map(_.action) should contain(ErrorAction.DeleteGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.App)
      getApp.nodepool.status shouldBe NodepoolStatus.Unspecified
    }

    val res = for {
      tr <- traceId.ask
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, gkeInterp = gkeInterp)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "handle an error in delete nodepool" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val exceptionMessage = "test exception"
    val mockAckConsumer = mock[AckReplyConsumer]

    val mockGKEService = new MockGKEService {
      override def deleteNodepool(nodepoolId: GKEModels.NodepoolId)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[v1.Operation] = IO.raiseError(new Exception(exceptionMessage))
    }
    val gkeInterp =
      new GKEInterpreter[IO](Config.gkeInterpConfig,
                             vpcInterp,
                             mockGKEService,
                             new MockKubernetesService(PodStatus.Succeeded),
                             MockHelm,
                             MockGalaxyDAO,
                             credentials,
                             iamDAOKubernetes,
                             blocker)

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.DeleteGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Nodepool)
      getApp.app.status shouldBe AppStatus.Error
      getApp.nodepool.status shouldBe NodepoolStatus.Error
    }

    val res = for {
      tr <- traceId.ask
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, gkeInterp = gkeInterp)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  //error on delete disk if disk doesn't exist
  it should "handle an error in delete app if delete disk = true and no disk exists" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.DeleteGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Disk)
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getApp.app.status shouldBe AppStatus.Error
    }

    val res = for {
      tr <- traceId.ask
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             Some(DiskId(-1)),
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "be idempotent" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      // send message twice
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }
}
