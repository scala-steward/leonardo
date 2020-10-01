package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import cats.effect.IO
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{makePersistentDisk, traceId}
import org.broadinstitute.dsde.workbench.leonardo.db.persistentDiskQuery
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{DeleteDiskMessage, _}

import scala.concurrent.ExecutionContext.Implicits.global

class LeoPubsubDiskMessageSpec extends LeoPubsubMessageSubscriberSpec {
  "CreateDiskMessage" should "create a disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      tr <- traceId.ask
      now <- IO(Instant.now)
      _ <- leoSubscriber.messageResponder(CreateDiskMessage.fromDisk(disk, Some(tr)))
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      updatedDisk.get.googleId.get.value shouldBe "target"
    }

    res.unsafeRunSync()
  }

  it should "be idempotent" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      tr <- traceId.ask
      now <- IO(Instant.now)

      message = CreateDiskMessage.fromDisk(disk, Some(tr))
      // send 2 messages
      _ <- leoSubscriber.messageResponder(message)
      _ <- leoSubscriber.messageResponder(message)
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      updatedDisk.get.googleId.get.value shouldBe "target"
    }

    res.unsafeRunSync()
  }

  "DeleteDiskMessage" should "delete a disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Deleting).save()
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(DeleteDiskMessage(disk.id, Some(tr)))
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      updatedDisk.get.status shouldBe DiskStatus.Deleting
    }

    res.unsafeRunSync()
  }

  it should "be a no-op when disk is not in Deleting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
      tr <- traceId.ask
      message = DeleteDiskMessage(disk.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Right(())
    }

    res.unsafeRunSync()
  }

  it should "be idempotent" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Deleting).save()
      tr <- traceId.ask

      message = DeleteDiskMessage(disk.id, Some(tr))
      // send 2 messages
      _ <- leoSubscriber.messageResponder(message)
      _ <- leoSubscriber.messageResponder(message)
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      updatedDisk.get.status shouldBe DiskStatus.Deleting
    }

    res.unsafeRunSync()
  }

  "UpdateDiskMessage" should "update disk size" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(UpdateDiskMessage(disk.id, DiskSize(550), Some(tr)))
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        for {
          updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
        } yield {
          updatedDisk shouldBe 'defined
          updatedDisk.get.size shouldBe DiskSize(550)
        }
      )
    } yield ()

    res.unsafeRunSync()
  }

}
