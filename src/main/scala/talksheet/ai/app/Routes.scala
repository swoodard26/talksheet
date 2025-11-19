package talksheet.ai.app

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

class Routes(
  uploadRoutes: UploadRoutes,
  chatRoutes: ChatRoutes
) {

  val routes: Route =
    pathPrefix("api") {
      uploadRoutes.route ~
      chatRoutes.route
    }
}
