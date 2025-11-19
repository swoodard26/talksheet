package talksheet.ai.app

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.SystemMaterializer
import akka.util.Timeout
import talksheet.ai.upload.{UploadCoordinator, XlsxParser}

import java.nio.file.Paths
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

object Main extends App {

  implicit val system: ActorSystem[Nothing] =
    ActorSystem[Nothing](Behaviors.empty, "xlsx-agent-system")

  implicit val ec: ExecutionContext =
    system.executionContext

  implicit val timeout: Timeout =
    10.seconds

  implicit val mat = SystemMaterializer(system).materializer

  // 1) Parser actor
  val parser: ActorRef[XlsxParser.Command] =
    system.systemActorOf(XlsxParser(), "xlsx-parser")

  // 2) UploadCoordinator actor with local storage dir
  val storageDir = Paths.get("data/uploads")
  val uploadCoordinator: ActorRef[UploadCoordinator.Command] =
    system.systemActorOf(
      UploadCoordinator(parser, storageDir),
      "upload-coordinator"
    )

  // 3) HTTP routes
  val uploadRoutes = new UploadRoutes(uploadCoordinator)
  val allRoutes: Route = new Routes(uploadRoutes).routes

  // 4) Bind HTTP server
  val bindingFut =
    Http()
      .newServerAt("0.0.0.0", 8080)
      .bind(allRoutes)

  bindingFut.onComplete {
    case Success(binding) =>
      system.log.info(
        "Server online at http://{}:{}/",
        binding.localAddress.getHostString,
        Int.box(binding.localAddress.getPort)
      )
    case Failure(ex) =>
      system.log.error("Failed to bind HTTP endpoint, terminating system", ex)
      system.terminate()
  }

  println("Server starting on http://localhost:8080/  (Press ENTER to stop)")
  StdIn.readLine()

  system.log.info("Shutting down...")
  system.terminate()
}
