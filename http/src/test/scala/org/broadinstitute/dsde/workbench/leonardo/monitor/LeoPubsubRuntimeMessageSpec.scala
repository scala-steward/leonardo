package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Operation
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.mock.MockComputePollOperation
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, OperationName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.VM
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.ClusterInvalidState
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{IP, TraceId}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Left

class LeoPubsubRuntimeMessageSpec extends LeoPubsubMessageSubscriberSpec {

  "CreateRuntimeMessage" should "create a runtime" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(
        makeCluster(1)
          .copy(asyncRuntimeFields = None, status = RuntimeStatus.Creating, serviceAccount = serviceAccount)
          .save()
      )
      tr <- traceId.ask
      gceRuntimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(gceRuntimeConfig).get
      _ <- leoSubscriber.messageResponder(CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfigRequest, Some(tr)))
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.asyncRuntimeFields shouldBe 'defined
      updatedRuntime.get.asyncRuntimeFields.get.stagingBucket.value should startWith("leostaging")
      updatedRuntime.get.asyncRuntimeFields.get.hostIp shouldBe None
      updatedRuntime.get.asyncRuntimeFields.get.operationName.value shouldBe "opName"
      updatedRuntime.get.asyncRuntimeFields.get.googleId.value shouldBe "target"
      updatedRuntime.get.runtimeImages.map(_.imageType) should contain(VM)
    }

    res.unsafeRunSync()
  }

  it should "be idempotent" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(
        makeCluster(1)
          .copy(asyncRuntimeFields = None, status = RuntimeStatus.Creating, serviceAccount = serviceAccount)
          .save()
      )
      tr <- traceId.ask
      gceRuntimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(gceRuntimeConfig).get
      message = CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfigRequest, Some(tr))
      // send 2 messages, we expect the same result as if we sent 1
      _ <- leoSubscriber.messageResponder(message)
      _ <- leoSubscriber.messageResponder(message)
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.asyncRuntimeFields shouldBe 'defined
      updatedRuntime.get.asyncRuntimeFields.get.stagingBucket.value should startWith("leostaging")
      updatedRuntime.get.asyncRuntimeFields.get.hostIp shouldBe None
      updatedRuntime.get.asyncRuntimeFields.get.operationName.value shouldBe "opName"
      updatedRuntime.get.asyncRuntimeFields.get.googleId.value shouldBe "target"
      updatedRuntime.get.runtimeImages.map(_.imageType) should contain(VM)
    }

    res.unsafeRunSync()
  }

  "DeleteRuntimeMessage" should "delete a runtime" in isolatedDbTest {
    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
      monitor = new MockRuntimeMonitor {
        override def pollCheck(a: CloudService)(
          googleProject: GoogleProject,
          runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
          operation: com.google.cloud.compute.v1.Operation,
          action: RuntimeStatus
        )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
          clusterQuery.completeDeletion(runtime.id, Instant.now()).transaction
      }
      queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
      leoSubscriber = makeLeoSubscriber(runtimeMonitor = monitor, asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

      _ <- leoSubscriber.messageResponder(DeleteRuntimeMessage(runtime.id, None, Some(tr)))
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        clusterQuery
          .getClusterStatus(runtime.id)
          .transaction
          .map(status => status shouldBe (Some(RuntimeStatus.Deleted)))
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "delete disk when autoDeleteDisks is set" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      disk <- makePersistentDisk(None, Some(FormattedBy.GCE)).save()
      runtimeConfig = RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                                    bootDiskSize = DiskSize(50),
                                                    persistentDiskId = Some(disk.id))

      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(runtimeConfig))
      tr <- traceId.ask
      _ <- leoSubscriber.messageResponder(
        DeleteRuntimeMessage(runtime.id, Some(disk.id), Some(tr))
      )
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        persistentDiskQuery.getStatus(disk.id).transaction.map(status => status shouldBe (Some(DiskStatus.Deleted)))
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "persist an error if it fails to delete disk" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val pollOperation = new MockComputePollOperation {
      override def getZoneOperation(project: GoogleProject, zoneName: ZoneName, operationName: OperationName)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Operation] = IO.pure(
        Operation.newBuilder().setId("op").setName("opName").setTargetId("target").setStatus("PENDING").build()
      )
    }
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, computePollOperation = pollOperation)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      disk <- makePersistentDisk(None, Some(FormattedBy.GCE)).save()
      runtimeConfig = RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                                    bootDiskSize = DiskSize(50),
                                                    persistentDiskId = Some(disk.id))

      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(runtimeConfig))
      tr <- traceId.ask
      _ <- leoSubscriber.messageResponder(DeleteRuntimeMessage(runtime.id, Some(disk.id), Some(tr)))
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        clusterErrorQuery.get(runtime.id).transaction.map { error =>
          val dummyNow = Instant.now()
          error.head.copy(timestamp = dummyNow) shouldBe RuntimeError(s"Fail to delete ${disk.name} in a timely manner",
                                                                      -1,
                                                                      dummyNow)
        }
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "fail when a runtime is not in Deleting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
      message = DeleteRuntimeMessage(runtime.id, None, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "be idempotent" in isolatedDbTest {
    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
      monitor = new MockRuntimeMonitor {
        override def pollCheck(a: CloudService)(
          googleProject: GoogleProject,
          runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
          operation: com.google.cloud.compute.v1.Operation,
          action: RuntimeStatus
        )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
          clusterQuery.completeDeletion(runtime.id, Instant.now()).transaction
      }
      queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
      leoSubscriber = makeLeoSubscriber(runtimeMonitor = monitor, asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

      message = DeleteRuntimeMessage(runtime.id, None, Some(tr))
      // send 2 messages, we expect the same result as if we sent one
      _ <- leoSubscriber.messageResponder(message)
      _ <- leoSubscriber.messageResponder(message)
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        clusterQuery
          .getClusterStatus(runtime.id)
          .transaction
          .map(status => status shouldBe (Some(RuntimeStatus.Deleted)))
      )
    } yield ()

    res.unsafeRunSync()
  }

  "StartRuntimeMessage" should "start a runtime" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Starting).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(StartRuntimeMessage(runtime.id, Some(tr)))
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Starting
    }

    res.unsafeRunSync()
  }

  it should "fail when runtime is not in Starting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
      message = StartRuntimeMessage(runtime.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "be idempotent" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Starting).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      message = StartRuntimeMessage(runtime.id, Some(tr))
      // send 2 messages
      _ <- leoSubscriber.messageResponder(message)
      _ <- leoSubscriber.messageResponder(message)
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Starting
    }

    res.unsafeRunSync()
  }

  "StopRuntimeMessage" should "stop a runtime" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopping).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(StopRuntimeMessage(runtime.id, Some(tr)))
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopping
    }

    res.unsafeRunSync()
  }

  it should "fail when runtime is not in Stopping status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
      message = StopRuntimeMessage(runtime.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "be idempotent" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopping).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      message = StopRuntimeMessage(runtime.id, Some(tr))
      // send 2 messages
      _ <- leoSubscriber.messageResponder(message)
      _ <- leoSubscriber.messageResponder(message)
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopping
    }

    res.unsafeRunSync()
  }

  "UpdateRuntimeMessage" should "resize dataproc cluster and set DB status properly" in isolatedDbTest {
    val monitor = new MockRuntimeMonitor {
      override def pollCheck(a: CloudService)(
        googleProject: GoogleProject,
        runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
        operation: com.google.cloud.compute.v1.Operation,
        action: RuntimeStatus
      )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.never

      override def process(
        a: CloudService
      )(runtimeId: Long, action: RuntimeStatus)(implicit ev: ApplicativeAsk[IO, TraceId]): Stream[IO, Unit] =
        Stream.eval(clusterQuery.setToRunning(runtimeId, IP("0.0.0.0"), Instant.now).transaction.void)
    }
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(runtimeMonitor = monitor, asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      runtime <- IO(
        makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(defaultDataprocRuntimeConfig)
      )
      tr <- traceId.ask
      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id, None, false, None, Some(100), None, Some(tr))
      )
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      updatedRuntimeConfig <- updatedRuntime.traverse(r =>
        RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
      )
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        clusterQuery.getClusterStatus(runtime.id).transaction.map(s => s shouldBe Some(RuntimeStatus.Running))
      )
    } yield {
      updatedRuntimeConfig.get.asInstanceOf[RuntimeConfig.DataprocConfig].numberOfWorkers shouldBe 100
    }

    res.unsafeRunSync()
  }

  it should "stop the cluster when there's a machine type change" in isolatedDbTest {
    val monitor = new MockRuntimeMonitor {
      override def pollCheck(a: CloudService)(
        googleProject: GoogleProject,
        runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
        operation: com.google.cloud.compute.v1.Operation,
        action: RuntimeStatus
      )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.never
    }
    val leoSubscriber = makeLeoSubscriber(monitor)

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id, Some(MachineTypeName("n1-highmem-64")), true, None, None, None, Some(tr))
      )
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      updatedRuntimeConfig <- updatedRuntime.traverse(r =>
        RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
      )
    } yield {
      // runtime should be Stopping
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopping
      // machine type should not be updated yet
      updatedRuntimeConfig shouldBe 'defined
      updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-standard-4")
    }

    res.unsafeRunSync()
  }

  it should "go through a stop-start transition for machine type" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id, Some(MachineTypeName("n1-highmem-64")), true, None, None, None, Some(tr))
      )
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        for {
          updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
          updatedRuntimeConfig <- updatedRuntime.traverse(r =>
            RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
          )
          patchInProgress <- patchQuery.isInprogress(runtime.id).transaction
        } yield {
          // runtime should be Starting after having gone through a stop -> update -> start
          updatedRuntime shouldBe 'defined
          updatedRuntime.get.status shouldBe RuntimeStatus.Starting
          // machine type should be updated
          updatedRuntimeConfig shouldBe 'defined
          updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-highmem-64")
          patchInProgress shouldBe false
        }
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "restart runtime for persistent disk size update" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      disk <- makePersistentDisk(None).copy(size = DiskSize(100)).save()
      runtime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Running)
          .saveWithRuntimeConfig(gceWithPdRuntimeConfig.copy(persistentDiskId = Some(disk.id)))
      )
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id,
                             None,
                             true,
                             Some(DiskUpdate.PdSizeUpdate(disk.id, disk.name, DiskSize(200))),
                             None,
                             None,
                             Some(tr))
      )

      assert = for {
        updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
        updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
      } yield {
        // runtime should be Starting after having gone through a stop -> start
        updatedRuntime shouldBe 'defined
        updatedRuntime.get.status shouldBe RuntimeStatus.Starting
        // machine type should be updated
        updatedDisk shouldBe 'defined
        updatedDisk.get.size shouldBe DiskSize(200)
      }

      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        assert
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "update diskSize should trigger a stop-start transition" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopped).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id,
                             Some(MachineTypeName("n1-highmem-64")),
                             false,
                             Some(DiskUpdate.NoPdSizeUpdate(DiskSize(1024))),
                             None,
                             None,
                             Some(tr))
      )
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      updatedRuntimeConfig <- updatedRuntime.traverse(r =>
        RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
      )
    } yield {
      // runtime should still be Stopped
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopped
      // machine type and disk size should be updated
      updatedRuntimeConfig shouldBe 'defined
      updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-highmem-64")
      updatedRuntimeConfig.get.asInstanceOf[RuntimeConfig.GceConfig].diskSize shouldBe DiskSize(1024)
    }

    res.unsafeRunSync()
  }
}
