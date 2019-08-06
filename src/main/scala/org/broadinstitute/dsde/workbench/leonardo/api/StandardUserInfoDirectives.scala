package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.headerValueByName
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}

/**
  * Created by rtitle on 10/16/17.
  */
trait StandardUserInfoDirectives extends UserInfoDirectives {
  override def requireUserInfo: Directive1[UserInfo] = {
    (headerValueByName("OIDC_access_token") &
     headerValueByName("OIDC_CLAIM_uid") &
     headerValueByName("OIDC_CLAIM_exp") &
     headerValueByName("OIDC_CLAIM_sub")).tmap { case (token, userId, expiresIn, email) =>
      UserInfo(OAuth2BearerToken(token), WorkbenchUserId(userId), WorkbenchEmail(email), expiresIn.toLong)
    }
  }
}
