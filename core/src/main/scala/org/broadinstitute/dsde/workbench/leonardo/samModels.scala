package org.broadinstitute.dsde.workbench.leonardo

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

sealed trait SamResourceType extends Product with Serializable {
  def asString: String
}
object SamResourceType {
  final case object Project extends SamResourceType {
    val asString = "billing-project"
  }
  final case object Runtime extends SamResourceType {
    val asString = "notebook-cluster"
  }
  final case object PersistentDisk extends SamResourceType {
    val asString = "persistent-disk"
  }
  val stringToSamResourceType: Map[String, SamResourceType] =
    sealerate.collect[SamResourceType].map(p => (p.asString, p)).toMap
}

sealed trait SamResource extends Product with Serializable {
  def resourceId: String
  def resourceType: SamResourceType
}
object SamResource {
//  final case class Project(resourceId: String) {
//    override def resourceType: SamResourceType.Project
//    def googleProject = GoogleProject(resourceId)
//  }
  final case class Runtime(resourceId: String) extends SamResource {
    val resourceType = SamResourceType.Runtime
  }
  final case class PersistentDisk(resourceId: String) extends SamResource {
    val resourceType = SamResourceType.PersistentDisk
  }
  final case class Project(googleProject: GoogleProject) extends SamResource {
    val resourceId = googleProject.value
    val resourceType = SamResourceType.Project
  }
}

//sealed trait AccessPolicyName extends Serializable with Product
//object AccessPolicyName {
//  final case object Creator extends AccessPolicyName {
//    override def toString = "creator"
//  }
//  final case object Owner extends AccessPolicyName {
//    override def toString = "owner"
//  }
//  final case class Other(asString: String) extends AccessPolicyName {
//    override def toString = asString
//  }
//
//  val stringToAccessPolicyName: Map[String, AccessPolicyName] =
//    sealerate.collect[AccessPolicyName].map(p => (p.toString, p)).toMap
//}
//
//sealed trait SamPolicy {
//  type SR
//  def policyName: AccessPolicyName
//  def resource: SR
//}
//object SamPolicy {
//  case class ProjectPolicy(resource: SamResource.Project, policyName: AccessPolicyName) extends SamPolicy {
//    type SR = SamResource.Project
//  }
//}