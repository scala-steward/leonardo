package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Operation
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.mock.MockComputePollOperation
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, OperationName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.ClusterInvalidState
import org.broadinstitute.dsde.workbench.leonardo.{
  AsyncTaskProcessor,
  CloudService,
  DiskSize,
  DiskStatus,
  FormattedBy,
  RuntimeAndRuntimeConfig,
  RuntimeConfig,
  RuntimeError,
  RuntimeStatus
}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext.Implicits.global

class LeoPubsubDeleteRuntimeMessageSpec extends LeoPubsubMessageSubscriberSpec {

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
    // TODO
  }

}
