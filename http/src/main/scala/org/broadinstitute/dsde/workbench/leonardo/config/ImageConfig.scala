package org.broadinstitute.dsde.workbench.leonardo
package config
import java.nio.file.Path

final case class ImageConfig(
  welderGcrImage: ContainerImage,
  welderDockerHubImage: ContainerImage,
  welderHash: String,
  jupyterImage: ContainerImage,
  proxyImage: ContainerImage,
  cryptoDetectorImage: ContainerImage,
  jupyterContainerName: String,
  rstudioContainerName: String,
  welderContainerName: String,
  proxyContainerName: String,
  cryptoDetectorContainerName: String,
  jupyterImageRegex: String,
  rstudioImageRegex: String,
  broadDockerhubImageRegex: String,
  defaultJupyterUserHome: Path
)
