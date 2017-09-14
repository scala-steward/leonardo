package org.broadinstitute.dsde.workbench.leonardo.config

case class DataprocConfig(applicationName: String,
                          serviceAccount: String,
                          dataprocDefaultRegion: String,
                          leoGoogleBucket: String,
                          dataprocDockerImage: String,
                          jupyterProxyDockerImage: String,
                          clusterFirewallRuleName: String,
                          clusterFirewallVPCNetwork: String,
                          clusterNetworkTag: String,
                          configFolderPath: String,
                          initActionsScriptName: String,
                          clusterDockerComposeName: String,
                          serviceAccountPemName: String,
                          jupyterServerCrtName: String,
                          jupyterServerKeyName: String,
                          jupyterRootCaPemName: String,
                          jupyterRootCaKeyName: String,
                          jupyterProxySiteConfName: String,
                          clusterUrlBase: String,
                          jupyterServerName: String,
                          proxyServerName: String)