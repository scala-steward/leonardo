package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.IO
import com.google.cloud.pubsub.v1.AckReplyConsumer
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.Event
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.{AsyncTaskProcessor, KubernetesClusterStatus, NodepoolStatus}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.mockito.Mockito.{verify, _}

import scala.concurrent.ExecutionContext.Implicits.global

class LeoPubsubBatchNodepoolMessageSpec extends LeoPubsubMessageSubscriberSpec {
  "BatchNodepoolCreateMessage" should "create a cluster and nodepools" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Precreating).save()
    val savedNodepool2 = makeNodepool(3, savedCluster1.id).copy(status = NodepoolStatus.Precreating).save()

    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getMinimalCluster <- kubernetesClusterQuery.getMinimalActiveClusterByName(savedCluster1.googleProject).transaction
    } yield {
      getMinimalCluster.get.status shouldBe KubernetesClusterStatus.Running
      getMinimalCluster.get.nodepools.size shouldBe 3
      getMinimalCluster.get.nodepools.filter(_.isDefault).size shouldBe 1
      getMinimalCluster.get.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getMinimalCluster.get.nodepools.filterNot(_.isDefault).size shouldBe 2
      getMinimalCluster.get.nodepools.filterNot(_.isDefault).map(_.status).distinct.size shouldBe 1
      getMinimalCluster.get.nodepools
        .filterNot(_.isDefault)
        .map(_.status)
        .distinct
        .head shouldBe NodepoolStatus.Unclaimed
    }

    val res = for {
      tr <- traceId.ask
      msg = BatchNodepoolCreateMessage(savedCluster1.id,
                                       List(savedNodepool1.id, savedNodepool2.id),
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
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "be idempotent" in isolatedDbTest {
    // TODO
  }

}
