package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, ClusterName, ClusterProjectAndName}
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.service.RestException

import scala.language.postfixOps


class NotebookLocalizeFileSpecWithWelder extends ClusterFixtureSpec with NotebookTestUtils {
  override def enableWelder: Boolean = true

  "Leonardo notebooks" - {
    "should localize files in sync mode" in { clusterFixture =>
      val localizeFileName = "localize_sync.txt"
      val localizeFileContents = "Sync localize test"
      val delocalizeFileName = "delocalize_sync.txt"
      val delocalizeFileContents = "Sync delocalize test"
      val localizeDataFileName = "localize_data_aync.txt"
      val localizeDataContents = "Hello World"

      withWebDriver { implicit driver =>
        val cluster = ClusterProjectAndName(GoogleProject("gpalloc-dev-master-5bqdytm"), ClusterName("automation-test-aesgpfisz"))//clusterFixture.cluster
//        val cluster = clusterFixture.cluster
        withLocalizeDelocalizeFiles(cluster, localizeFileName, localizeFileContents, delocalizeFileName, delocalizeFileContents, localizeDataFileName, localizeDataContents) { (localizeRequest, bucketName, notebookPage) =>
          // call localize; this should return 200
          val res=  Notebook.localize(cluster.googleProject, cluster.clusterName, localizeRequest, async = false)
          println(s"1111: ${res}")

          // check that the files are immediately at their destinations
          verifyLocalizeDelocalize(cluster, localizeFileName, localizeFileContents, GcsPath(bucketName, GcsObjectName(delocalizeFileName)), delocalizeFileContents, localizeDataFileName, localizeDataContents)

          // call localize again with bad data. This should still return 500 since we're in sync mode.
          val badLocalize = Map("file.out" -> "gs://nobuckethere")
          val thrown = the[RestException] thrownBy {
            Notebook.localize(cluster.googleProject, cluster.clusterName, badLocalize, async = false)
          }
          // why doesn't `RestException` have a status code field?
          thrown.message should include("500 : Internal Server Error")
          thrown.message should include("Error occurred localizing")
          thrown.message should include("See localization.log for details.")

          // it should not have localized this file
          val contentThrown = the[RestException] thrownBy {
            Notebook.getContentItem(cluster.googleProject, cluster.clusterName, "file.out", includeContent = false)
          }
          contentThrown.message should include("No such file or directory: file.out")
        }
      }
    }
  }

}
