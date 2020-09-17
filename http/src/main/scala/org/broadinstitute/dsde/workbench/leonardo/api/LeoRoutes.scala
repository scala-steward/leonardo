package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.Materializer
import cats.effect.{ContextShift, IO}
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutesJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutesSprayJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.RouteValidation._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeRequest, LeonardoService}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext
case class AuthenticationError(email: Option[WorkbenchEmail] = None)
    extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is not authenticated",
                         StatusCodes.Unauthorized)

// TODO: This can probably renamed to legacyRuntimeRoutes
// Future runtime related APIs should be added to `RuntimeRoutes`
class LeoRoutes(
  val leonardoService: LeonardoService,
  userInfoDirectives: UserInfoDirectives
)(implicit val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext,
  val cs: ContextShift[IO])
    extends LazyLogging {

  val route: Route =
    userInfoDirectives.requireUserInfo { userInfo =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))

      CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
        pathPrefix("cluster") {
          pathPrefix("v2" / Segment / Segment) { (googleProject, clusterNameString) =>
            validateNameDirective(clusterNameString, RuntimeName.apply) { clusterName =>
              pathEndOrSingleSlash {
                put {
                  entity(as[CreateRuntimeRequest]) { cluster =>
                    complete {
                      leonardoService
                        .createCluster(userInfo, GoogleProject(googleProject), clusterName, cluster)
                        .map(cluster => StatusCodes.Accepted -> cluster)
                    }
                  }
                }
              }
            }
          } ~
            pathPrefix(Segment / Segment) { (googleProject, clusterNameString) =>
              validateNameDirective(clusterNameString, RuntimeName.apply) { clusterName =>
                pathEndOrSingleSlash {
                  put {
                    entity(as[CreateRuntimeRequest]) { cluster =>
                      complete {
                        leonardoService
                          .createCluster(userInfo, GoogleProject(googleProject), clusterName, cluster)
                          .map(cluster => StatusCodes.OK -> cluster)
                      }
                    }
                  }
                }
              }
            }
        }
      }
    }
}
