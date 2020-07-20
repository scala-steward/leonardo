package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceName}
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.GceMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  LeoPubsubMessageSubscriberConfig,
  PersistentDiskMonitor,
  PersistentDiskMonitorConfig
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ConfigSpec extends AnyFlatSpec with Matchers {
  it should "read PersistentDiskConfig properly" in {
    val expectedResult = PersistentDiskConfig(
      DiskSize(30),
      DiskType.Standard,
      BlockSize(4096),
      ZoneName("us-central1-a")
    )

    Config.persistentDiskConfig shouldBe expectedResult
  }

  it should "read LeoPubsubMessageSubscriberConfig properly" in {
    val expectedResult = LeoPubsubMessageSubscriberConfig(
      100,
      295 seconds,
      PersistentDiskMonitorConfig(
        PersistentDiskMonitor(5, 3 seconds),
        PersistentDiskMonitor(5, 3 seconds),
        PersistentDiskMonitor(5, 3 seconds)
      )
    )

    Config.leoPubsubMessageSubscriberConfig shouldBe expectedResult
  }

  it should "read gce.monitor properly" in {
    val expected = GceMonitorConfig(
      20 seconds,
      15 seconds,
      120,
      8 seconds,
      Config.clusterBucketConfig,
      Map(
        RuntimeStatus.Creating -> 30.minutes,
        RuntimeStatus.Starting -> 20.minutes,
        RuntimeStatus.Deleting -> 30.minutes
      ),
      ZoneName("us-central1-a"),
      Config.imageConfig
    )

    Config.gceMonitorConfig shouldBe (expected)
  }

  "GKE config" should "read ClusterConfig properly" in {
    val expectedResult = KubernetesClusterConfig(Location("us-central1-a"))
    Config.gkeClusterConfig shouldBe expectedResult
  }

  it should "read DefaultNodepoolConfig properly" in {
    val expectedResult = DefaultNodepoolConfig(MachineTypeName("g1-small"), NumNodes(1), false)
    Config.gkeDefaultNodepoolConfig shouldBe expectedResult
  }

  it should "read GalaxyNodepoolConfig properly" in {
    val expectedResult = GalaxyNodepoolConfig(MachineTypeName("n2-standard-8"),
                                              NumNodes(1),
                                              true,
                                              AutoscalingConfig(AutoscalingMin(0), AutoscalingMax(1)))
    Config.gkeGalaxyNodepoolConfig shouldBe expectedResult
  }

  it should "read GalaxyAppConfig properly" in {
    val expectedResult = GalaxyAppConfig(
      ReleaseName("release1"),
      NamespaceName("namespace"),
      List(ServiceConfig(ServiceName("galaxy-web"), KubernetesServiceKindName("ClusterIP"))),
      RemoteUserName("galaxy-user")
    )
    Config.gkeGalaxyAppConfig shouldBe expectedResult
  }
}