import java.sql.Timestamp

import org.broadinstitute.dsde.workbench.leonardo.db.{InstanceRecord, LeoProfile, clusterQuery}
import LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterFollowupDetails

import scala.concurrent.ExecutionContext

case class FollowupRecord(/*id: Long,*/
                          clusterId: Long,
                          status: String,
                          masterMachineType: Option[String])

class FollowupTable(tag: Tag) extends Table[FollowupRecord](tag, "CLUSTER_FOLLOWUP") {
//  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  //TODO: should I make a compound primary key over clusterId + status?
  def clusterId = column[Long]("clusterId", O.PrimaryKey)
  def status = column[String]("status", O.Length(254))
  def masterMachineType = column[Option[String]]("dataprocRole", O.Length(254))


  def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

  def * =
    (clusterId, status, masterMachineType) <> (FollowupRecord.tupled, FollowupRecord.unapply)
}

object followupQuery extends TableQuery(new FollowupTable(_)) {
  def save(followupDetails: ClusterFollowupDetails, masterMachineType: String): DBIO[Int] =
    followupQuery += FollowupRecord(followupDetails.clusterId, followupDetails.clusterStatus.entryName, Some(masterMachineType))

  def delete(followupDetails: ClusterFollowupDetails): DBIO[Int] =
    baseFollowupQuery(followupDetails)
      .delete

  def hasFollowupDetails(followupDetails: ClusterFollowupDetails): DBIO[Boolean] = {
    baseFollowupQuery(followupDetails)
      .exists
      .result
  }

  //TODO: should this differentiate itself between no cluster with that ID found, and no machine config found?
  //gets the masterMachineType field for a given
  def getFollowupDetails(followupDetails: ClusterFollowupDetails)(implicit ec: ExecutionContext): DBIO[Option[String]] = {
      baseFollowupQuery(followupDetails)
        .result
        .headOption
          .map {
            recordOpt => recordOpt match {
              case Some(record) => record.masterMachineType
              case None => None
            }
          }
  }
  private def baseFollowupQuery(followupDetails: ClusterFollowupDetails) =
    followupQuery
      .filter( _.clusterId == followupDetails.clusterId)
}
