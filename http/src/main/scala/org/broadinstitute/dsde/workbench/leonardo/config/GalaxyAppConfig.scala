package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.{KubernetesServiceAccount, ServiceConfig}
import org.broadinstitute.dsp.{ChartName, ChartVersion, Release}

final case class GalaxyAppConfig(release: Release,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: String,
                                 services: List[ServiceConfig],
                                 serviceAccount: KubernetesServiceAccount,
                                 uninstallKeepHistory: Boolean) {

  def chartInfo: String = s"${chartName.asString}-${chartVersion.asString}"
}
