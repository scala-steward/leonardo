package org.broadinstitute.dsde.workbench.leonardo.model

import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import cats.Semigroup
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.StringValueClass._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.GoogleProjectFormat
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject, ServiceAccountKey, _}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat, SerializationException}

import scala.language.implicitConversions

// This needs to be a Universal Trait to enable mixin with Scala Value Classes (AnyVal)
sealed trait StringValueClass extends Any with Product with Serializable {
  def value: String

  // productPrefix makes toString = e.g. "Cluster (clustername)"
  override def productPrefix: String = getClass.getSimpleName + " "
}
object StringValueClass {
  type LabelMap = Map[String, String]
}

// Google primitives
case class ClusterName(value: String) extends AnyVal with StringValueClass
case class OperationName(value: String) extends AnyVal with StringValueClass
case class Operation(name: OperationName, uuid: UUID)
case class InstanceName(value: String) extends AnyVal with StringValueClass
case class ClusterResource(value: String) extends AnyVal with StringValueClass
case class IP(value: String) extends AnyVal with StringValueClass
case class ZoneUri(value: String) extends AnyVal with StringValueClass
case class NetworkTag(value: String) extends AnyVal with StringValueClass

// Google Firewall rules
case class FirewallRuleName(value: String) extends AnyVal with StringValueClass
case class FirewallRulePort(value: String) extends AnyVal with StringValueClass
case class FirewallRuleNetwork(value: String) extends AnyVal with StringValueClass
case class FirewallRule(name: FirewallRuleName, protocol: String = "tcp", ports: List[FirewallRulePort], network: FirewallRuleNetwork, targetTags: List[NetworkTag])

// Cluster default labels
case class DefaultLabels(clusterName: ClusterName,
                         googleProject: GoogleProject,
                         creator: WorkbenchEmail,
                         clusterServiceAccount: Option[WorkbenchEmail],
                         notebookServiceAccount: Option[WorkbenchEmail],
                         notebookExtension: Option[GcsPath])

// Service account information
case class ServiceAccountInfo(clusterServiceAccount: Option[WorkbenchEmail],
                              notebookServiceAccount: Option[WorkbenchEmail])

// Information about cluster creation errors
case class ClusterErrorDetails(code: Int, message: Option[String])

// ClusterStatus enum
object ClusterStatus extends Enumeration {
  type ClusterStatus = Value
  //NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  val Unknown, Creating, Running, Updating, Error, Deleting, Deleted = Value

  val activeStatuses = Set(Unknown, Creating, Running, Updating)
  val deletableStatuses = Set(Unknown, Creating, Running, Updating, Error)
  val monitoredStatuses = Set(Unknown, Creating, Updating, Deleting)

  class StatusValue(status: ClusterStatus) {
    def isActive: Boolean = activeStatuses contains status
    def isMonitored: Boolean = monitoredStatuses contains status
    def isDeletable: Boolean = deletableStatuses contains status
  }
  implicit def enumConvert(status: ClusterStatus): StatusValue = new StatusValue(status)

  def withNameOpt(s: String): Option[ClusterStatus] = values.find(_.toString == s)

  def withNameIgnoreCase(str: String): ClusterStatus = {
    values.find(_.toString.equalsIgnoreCase(str)).getOrElse(throw new IllegalArgumentException(s"Unknown cluster status: $str"))
  }
}

// Create cluster request
case class ClusterRequest(labels: LabelMap = Map.empty,
                          jupyterExtensionUri: Option[GcsPath] = None,
                          machineConfig: Option[MachineConfig] = None)

// Cluster
case class Cluster(clusterName: ClusterName,
                   googleId: UUID,
                   googleProject: GoogleProject,
                   serviceAccountInfo: ServiceAccountInfo,
                   machineConfig: MachineConfig,
                   clusterUrl: URL,
                   operationName: OperationName,
                   status: ClusterStatus,
                   hostIp: Option[IP],
                   creator: WorkbenchEmail,
                   createdDate: Instant,
                   destroyedDate: Option[Instant],
                   labels: LabelMap,
                   jupyterExtensionUri: Option[GcsPath]) {
  def projectNameString: String = s"${googleProject.value}/${clusterName.value}"
}

object Cluster {
  def create(clusterRequest: ClusterRequest,
             userEmail: WorkbenchEmail,
             clusterName: ClusterName,
             googleProject: GoogleProject,
             operation: Operation,
             serviceAccountInfo: ServiceAccountInfo,
             machineConfig: MachineConfig,
             clusterUrlBase: String): Cluster = {
    Cluster(
        clusterName = clusterName,
        googleId = operation.uuid,
        googleProject = googleProject,
        serviceAccountInfo = serviceAccountInfo,
        machineConfig = machineConfig,
        clusterUrl = getClusterUrl(clusterUrlBase, googleProject, clusterName),
        operationName = operation.name,
        status = ClusterStatus.Creating,
        hostIp = None,
        userEmail,
        createdDate = Instant.now(),
        destroyedDate = None,
        labels = clusterRequest.labels,
        jupyterExtensionUri = clusterRequest.jupyterExtensionUri
      )
  }

  def createDummyForDeletion(clusterRequest: ClusterRequest,
                             userEmail: WorkbenchEmail,
                             clusterName: ClusterName,
                             googleProject: GoogleProject,
                             serviceAccountInfo: ServiceAccountInfo): Cluster = {
    Cluster(
      clusterName = clusterName,
      googleId = UUID.randomUUID,
      googleProject = googleProject,
      serviceAccountInfo = serviceAccountInfo,
      machineConfig = MachineConfig(clusterRequest.machineConfig, ClusterDefaultsConfig(0, "", 0, "", 0, 0, 0)),
      clusterUrl = getClusterUrl("https://dummy-cluster/", googleProject, clusterName),
      operationName = OperationName("dummy-operation"),
      status = ClusterStatus.Creating,
      hostIp = None,
      userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = clusterRequest.labels,
      jupyterExtensionUri = clusterRequest.jupyterExtensionUri
    )
  }

  def getClusterUrl(clusterUrlBase: String, googleProject: GoogleProject, clusterName: ClusterName): URL = {
    new URL(clusterUrlBase + googleProject.value + "/" + clusterName.value)
  }
}

// Machine configuration
case class NegativeIntegerArgumentInClusterRequestException()
  extends LeoException(s"Your cluster request should not have negative integer values. Please revise your request and submit again.", StatusCodes.BadRequest)

case class OneWorkerSpecifiedInClusterRequestException()
  extends LeoException("Google Dataproc does not support clusters with 1 non-preemptible worker. Must be 0, 2 or more.")

case class MachineConfig(numberOfWorkers: Option[Int] = None,
                         masterMachineType: Option[String] = None,
                         masterDiskSize: Option[Int] = None,  //min 10
                         workerMachineType: Option[String] = None,
                         workerDiskSize: Option[Int] = None,   //min 10
                         numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                         numberOfPreemptibleWorkers: Option[Int] = None)

object MachineConfig {

  implicit val machineConfigSemigroup = new Semigroup[MachineConfig] {
    def combine(defined: MachineConfig, default: MachineConfig): MachineConfig = {
      val minimumDiskSize = 100
      defined.numberOfWorkers match {
        case None | Some(0) => MachineConfig(Some(0), defined.masterMachineType.orElse(default.masterMachineType),
          checkNegativeValue(defined.masterDiskSize.orElse(default.masterDiskSize)).map(s => math.max(minimumDiskSize, s)))
        case Some(numWorkers) if numWorkers == 1 => throw OneWorkerSpecifiedInClusterRequestException()
        case numWorkers => MachineConfig(checkNegativeValue(numWorkers),
          defined.masterMachineType.orElse(default.masterMachineType),
          checkNegativeValue(defined.masterDiskSize.orElse(default.masterDiskSize)).map(s => math.max(minimumDiskSize, s)),
          defined.workerMachineType.orElse(default.workerMachineType),
          checkNegativeValue(defined.workerDiskSize.orElse(default.workerDiskSize)).map(s => math.max(minimumDiskSize, s)),
          checkNegativeValue(defined.numberOfWorkerLocalSSDs.orElse(default.numberOfWorkerLocalSSDs)),
          checkNegativeValue(defined.numberOfPreemptibleWorkers.orElse(default.numberOfPreemptibleWorkers)))
      }
    }
  }

  def checkNegativeValue(value: Option[Int]): Option[Int] = {
    value.map(v => if (v < 0) throw NegativeIntegerArgumentInClusterRequestException() else v)
  }

  def apply(definedMachineConfig: Option[MachineConfig], defaultMachineConfig: ClusterDefaultsConfig): MachineConfig = {
    definedMachineConfig.getOrElse(MachineConfig()) |+| MachineConfig(defaultMachineConfig)
  }

  def apply(clusterDefaultsConfig: ClusterDefaultsConfig): MachineConfig = MachineConfig(
    Some(clusterDefaultsConfig.numberOfWorkers),
    Some(clusterDefaultsConfig.masterMachineType),
    Some(clusterDefaultsConfig.masterDiskSize),
    Some(clusterDefaultsConfig.workerMachineType),
    Some(clusterDefaultsConfig.workerDiskSize),
    Some(clusterDefaultsConfig.numberOfWorkerLocalSSDs),
    Some(clusterDefaultsConfig.numberOfPreemptibleWorkers)
  )
}

// Cluster init values
object ClusterInitValues {
  val serviceAccountCredentialsFilename = "service-account-credentials.json"

  def apply(googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, clusterRequest: ClusterRequest, dataprocConfig: DataprocConfig,
            clusterFilesConfig: ClusterFilesConfig, clusterResourcesConfig: ClusterResourcesConfig, proxyConfig: ProxyConfig,
            serviceAccountKey: Option[ServiceAccountKey], userEmailLoginHint: WorkbenchEmail): ClusterInitValues =
    ClusterInitValues(
      googleProject.value,
      clusterName.value,
      dataprocConfig.dataprocDockerImage,
      proxyConfig.jupyterProxyDockerImage,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterServerCrt.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterServerKey.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterRootCaPem.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.clusterDockerCompose.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterProxySiteConf.value)).toUri,
      dataprocConfig.jupyterServerName,
      proxyConfig.proxyServerName,
      clusterRequest.jupyterExtensionUri.map(_.toUri).getOrElse(""),
      serviceAccountKey.map(_ => GcsPath(initBucketName, GcsObjectName(serviceAccountCredentialsFilename)).toUri).getOrElse(""),
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterCustomJs.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterGoogleSignInJs.value)).toUri,
      userEmailLoginHint.value
    )
}

// see https://broadinstitute.atlassian.net/browse/GAWB-2619 for why these are Strings rather than value classes

case class ClusterInitValues(googleProject: String,
                             clusterName: String,
                             jupyterDockerImage: String,
                             proxyDockerImage: String,
                             jupyterServerCrt: String,
                             jupyterServerKey: String,
                             rootCaPem: String,
                             jupyterDockerCompose: String,
                             jupyterProxySiteConf: String,
                             jupyterServerName: String,
                             proxyServerName: String,
                             jupyterExtensionUri: String,
                             jupyterServiceAccountCredentials: String,
                             jupyterCustomJsUri: String,
                             jupyterGoogleSignInJsUri: String,
                             userEmailLoginHint: String)




object LeonardoJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit object UUIDFormat extends JsonFormat[UUID] {
    def write(obj: UUID) = JsString(obj.toString)

    def read(json: JsValue): UUID = json match {
      case JsString(uuid) => UUID.fromString(uuid)
      case other => throw DeserializationException("Expected UUID, got: " + other)
    }
  }

  implicit object InstantFormat extends JsonFormat[Instant] {
    def write(obj: Instant) = JsString(obj.toString)

    def read(json: JsValue): Instant = json match {
      case JsString(instant) => Instant.parse(instant)
      case other => throw DeserializationException("Expected Instant, got: " + other)
    }
  }

  implicit object ClusterStatusFormat extends JsonFormat[ClusterStatus] {
    def write(obj: ClusterStatus) = JsString(obj.toString)

    def read(json: JsValue): ClusterStatus = json match {
      case JsString(status) => ClusterStatus.withName(status)
      case other => throw DeserializationException("Expected ClusterStatus, got: " + other)
    }
  }

  implicit object URLFormat extends JsonFormat[URL] {
    def write(obj: URL) = JsString(obj.toString)

    def read(json: JsValue): URL = json match {
      case JsString(url) => new URL(url)
      case other => throw DeserializationException("Expected URL, got: " + other)
    }
  }

  implicit object GcsBucketNameFormat extends JsonFormat[GcsBucketName] {
    def write(obj: GcsBucketName) = JsString(obj.value)

    def read(json: JsValue): GcsBucketName = json match {
      case JsString(bucketName) => GcsBucketName(bucketName)
      case other => throw DeserializationException("Expected GcsBucketName, got: " + other)
    }
  }

  implicit object GcsPathFormat extends JsonFormat[GcsPath] {
    def write(obj: GcsPath) = JsString(obj.toUri)

    def read(json: JsValue): GcsPath = json match {
      case JsString(path) => parseGcsPath(path) match {
        case Right(gcsPath) => gcsPath
        case Left(gcsParseError) => throw DeserializationException(gcsParseError.value)
      }
      case other => throw DeserializationException("Expected GcsPath, got: " + other)
    }
  }

  case class StringValueClassFormat[T <: StringValueClass](apply: String => T, unapply: T => Option[String]) extends JsonFormat[T] {
    def write(obj: T): JsValue = unapply(obj) match {
      case Some(s) => JsString(s)
      case other => throw new SerializationException("Expected String, got: " + other)
    }

    def read(json: JsValue): T = json match {
      case JsString(value) => apply(value)
      case other => throw DeserializationException("Expected StringValueClass, got: " + other)
    }
  }

  implicit val clusterNameFormat = StringValueClassFormat(ClusterName, ClusterName.unapply)
  implicit val operationNameFormat = StringValueClassFormat(OperationName, OperationName.unapply)
  implicit val ipFormat = StringValueClassFormat(IP, IP.unapply)
  implicit val firewallRuleNameFormat = StringValueClassFormat(FirewallRuleName, FirewallRuleName.unapply)
  implicit val machineConfigFormat = jsonFormat7(MachineConfig.apply)
  implicit val serviceAccountInfoFormat = jsonFormat2(ServiceAccountInfo.apply)
  implicit val clusterFormat = jsonFormat14(Cluster.apply)
  implicit val clusterRequestFormat = jsonFormat3(ClusterRequest)
  implicit val clusterInitValuesFormat = jsonFormat16(ClusterInitValues.apply)
  implicit val defaultLabelsFormat = jsonFormat6(DefaultLabels.apply)
}
