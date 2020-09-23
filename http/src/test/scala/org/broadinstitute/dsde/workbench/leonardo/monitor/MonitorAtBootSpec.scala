package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.IO
import cats.Eq
import cats.implicits._
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.{
  AppStatus,
  DiskStatus,
  KubernetesClusterStatus,
  LeoLenses,
  LeonardoTestSuite,
  NodepoolStatus,
  RuntimeStatus
}
import org.broadinstitute.dsde.workbench.leonardo.db.{kubernetesClusterQuery, nodepoolQuery, TestComponent}
import org.scalatest.flatspec.AnyFlatSpec
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{
  makeApp,
  makeKubeCluster,
  makeNodepool,
  makeService
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterNodepoolAction.CreateClusterAndNodepool
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateAppMessage
import org.scalatest.Assertions

import scala.concurrent.ExecutionContext.Implicits.global

class MonitorAtBootSpec extends AnyFlatSpec with TestComponent with LeonardoTestSuite {
  implicit val msgEq: Eq[LeoPubsubMessage] =
    Eq.instance[LeoPubsubMessage]((x, y) =>
      (x, y) match {
        case (xx: LeoPubsubMessage.StopRuntimeMessage, yy: LeoPubsubMessage.StopRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.DeleteRuntimeMessage, yy: LeoPubsubMessage.DeleteRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.CreateRuntimeMessage, yy: LeoPubsubMessage.CreateRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.StartRuntimeMessage, yy: LeoPubsubMessage.StartRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.CreateAppMessage, yy: LeoPubsubMessage.CreateAppMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.DeleteAppMessage, yy: LeoPubsubMessage.DeleteAppMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx, yy) =>
          Assertions.fail(s"unexpected messages ${xx}, ${yy}", null)
      }
    )

  it should "recover RuntimeStatus.Stopping properly" in isolatedDbTest {
    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Stopping).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryDequeue1
    } yield {
      (msg eqv Some(LeoPubsubMessage.StopRuntimeMessage(runtime.id, None))) shouldBe (true)
    }
    res.unsafeRunSync()
  }

  it should "recover RuntimeStatus.Deleting properly" in isolatedDbTest {
    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Deleting).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryDequeue1
    } yield {
      (msg eqv Some(LeoPubsubMessage.DeleteRuntimeMessage(runtime.id, None, None))) shouldBe (true)
    }
    res.unsafeRunSync()
  }

  it should "recover RuntimeStatus.Starting properly" in isolatedDbTest {
    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Starting).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryDequeue1
    } yield {
      (msg eqv Some(LeoPubsubMessage.StartRuntimeMessage(runtime.id, None))) shouldBe (true)
    }
    res.unsafeRunSync()
  }

  it should "recover RuntimeStatus.Creating properly" in isolatedDbTest {
    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryDequeue1
    } yield {
      val runtimeConfigInCreateRuntimeMessage = LeoLenses.runtimeConfigPrism.getOption(defaultDataprocRuntimeConfig).get
      (msg eqv Some(
        LeoPubsubMessage.CreateRuntimeMessage.fromRuntime(
          runtime,
          runtimeConfigInCreateRuntimeMessage,
          None
        )
      )) shouldBe (true)
    }
    res.unsafeRunSync()
  }

  it should "recover AppStatus.Provisioning properly with cluster and nodepool creation" in isolatedDbTest {
    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      cluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Provisioning).save())
      nodepool <- IO(makeNodepool(2, cluster.id).copy(status = NodepoolStatus.Provisioning).save())
      _ <- IO(println("YYY " + cluster.nodepools.map(n => s"${n.id} - ${n.isDefault}").mkString(", ")))
      defaultNodepool = cluster.nodepools.find(_.isDefault).get
      app <- IO(makeApp(1, nodepool.id).copy(status = AppStatus.Provisioning).save())
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryDequeue1
    } yield {
      val expected = CreateAppMessage(cluster.googleProject,
                                      Some(CreateClusterAndNodepool(cluster.id, defaultNodepool.id, nodepool.id)),
                                      app.id,
                                      app.appName,
                                      Some(disk.id),
                                      Map.empty,
                                      None)

      println(s"got: $msg")
      println(s"expected: $msg")
      (msg eqv Some(expected)) shouldBe true
    }
    res.unsafeRunSync()
  }

  // TODO

  it should "recover AppStatus.Provisioning properly with nodepool creation" in isolatedDbTest {}

  it should "recover AppStatus.Provisioning properly" in isolatedDbTest {}

  it should "recover AppStatus.Deleting properly" in isolatedDbTest {}

  it should "ignore non-monitored apps" in isolatedDbTest {}

  def createMonitorAtBoot(
    queue: InspectableQueue[IO, LeoPubsubMessage] = InspectableQueue.bounded[IO, LeoPubsubMessage](10).unsafeRunSync
  ): MonitorAtBoot[IO] =
    new MonitorAtBoot[IO](queue, org.broadinstitute.dsde.workbench.errorReporting.FakeErrorReporting)
}
