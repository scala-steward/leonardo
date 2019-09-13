package org.broadinstitute.dsde.workbench.leonardo.notebooks

import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.leonardo.{ClusterFixtureSpec, LeonardoConfig}
import org.broadinstitute.dsde.workbench.service.util.Tags
import org.scalatest.DoNotDiscover

/**
  * This spec verifies Hail and Spark functionality.
  */
@DoNotDiscover
class NotebookHailSpec extends ClusterFixtureSpec with NotebookTestUtils {

  // Should match the HAILHASH env var in the Jupyter Dockerfile
  val expectedHailVersion = "devel-9d6bf0096349"
  val hailTutorialUploadFile = ResourceFile(s"diff-tests/hail-tutorial.ipynb")
  override val jupyterDockerImage: Option[String] = Some(LeonardoConfig.Leonardo.hailImageUrl)


  "NotebookHailSpec" - {
    "should install the right Hail version" taggedAs Tags.SmokeTest in { clusterFixture =>
      withWebDriver { implicit driver =>
<<<<<<< HEAD
        withNewNotebook(clusterFixture.cluster, Python3) { notebookPage =>
=======
        withNewNotebook(clusterFixture, Python3) { notebookPage =>
>>>>>>> refactor hailspec
          // Verify we have the right hail version
          val importHail =
            """import hail as hl
              |hl.init()
            """.stripMargin

          val importHailOutput =
            s"""Welcome to
               |     __  __     <>__
               |    / /_/ /__  __/ /
               |   / __  / _ `/ / /
               |  /_/ /_/\\_,_/_/_/   version $expectedHailVersion""".stripMargin

          notebookPage.executeCell(importHail, cellNumberOpt = Some(1)).get should include (importHailOutput)

          // Run the Hail tutorial and verify
          // https://hail.is/docs/0.2/tutorials-landing.html
          val tutorialToRun =
            """
              |hl.utils.get_movie_lens('data/')
              |users = hl.read_table('data/users.ht')
              |users.aggregate(hl.agg.count())""".stripMargin
          val tutorialCellResult = notebookPage.executeCellWithCellOutput(tutorialToRun, cellNumberOpt = Some(2)).get
          tutorialCellResult.output.get.toInt shouldBe(943)

          // Verify spark job works
          val sparkJobToSucceed =
            """import random
              |hl.balding_nichols_model(3, 1000, 1000)._force_count_rows()""".stripMargin
          val sparkJobToSucceedcellResult = notebookPage.executeCellWithCellOutput(sparkJobToSucceed, cellNumberOpt = Some(3)).get
          sparkJobToSucceedcellResult.output.get.toInt shouldBe(1000)
<<<<<<< HEAD
        }
      }
    }
<<<<<<< HEAD
=======

    val sparkJobToSucceed =
      """import random
        |import hail as hl
        |sc = hl.spark_context()
        |NUM_SAMPLES=20
        |def inside(p):
        |    x, y = random.random(), random.random()
        |    return x*x + y*y < 1
        |
        |count = sc.parallelize(range(0, NUM_SAMPLES)) \
        |             .filter(inside).count()
        |print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))""".stripMargin


    s"should be able to run a Spark job with a ${Python3.string} kernel" in { clusterFixture =>
      withWebDriver { implicit driver =>
        withNewNotebook(clusterFixture.cluster, Python3) { notebookPage =>
          val cellResult = notebookPage.executeCell(sparkJobToSucceed).get
          cellResult should include("Pi is roughly ")
          cellResult.toLowerCase should not include "error"
        }
      }
    }
<<<<<<< HEAD

>>>>>>> fix automation tests
=======
>>>>>>> more fix
=======
        }
      }
    }
>>>>>>> refactor hailspec
  }
}
