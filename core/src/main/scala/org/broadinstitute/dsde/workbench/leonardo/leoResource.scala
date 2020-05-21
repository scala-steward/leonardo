package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant

import org.broadinstitute.dsde.workbench.model.google.GoogleProject

abstract class LeoResource(id: LeoId,
                           samResourceId: SamResourceId,
                           name: ResourceName,
                           googleProject: GoogleProject,
                           auditInfo: AuditInfo,
                           labels: LabelMap,
                           errors: List[ResourceError]) {
  final def projectNameString = s"${googleProject.value}/${name}"
}

abstract class LeoId(asLong: Long)
abstract class SamResourceId(asString: String)
abstract class ResourceName(asString: String)
abstract class ResourceError(errorMessage: String, errorCode: Int, timestamp: Instant)
abstract class ResourceStatus
