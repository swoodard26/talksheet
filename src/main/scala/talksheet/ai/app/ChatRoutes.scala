package talksheet.ai.app

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.util.Timeout

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import spray.json.DefaultJsonProtocol._

class ChatRoutes(
  llm: LocalLlm
)(implicit
  system: ActorSystem[?],
  timeout: Timeout,
  ec: ExecutionContext
) {

  private final case class ChatRequest(uploadId: String, question: String)
  private final case class ColumnView(name: String, dataType: String)
  private final case class ChatResponse(sql: String, columns: Seq[ColumnView], rows: Seq[Map[String, String]])
  private final case class ErrorResponse(error: String)

  private implicit val chatRequestFormat = jsonFormat2(ChatRequest)
  private implicit val columnViewFormat = jsonFormat2(ColumnView)
  private implicit val chatResponseFormat = jsonFormat3(ChatResponse)
  private implicit val errorResponseFormat = jsonFormat1(ErrorResponse)

  val route: Route =
    path("chat") {
      post {
        entity(as[ChatRequest]) { request =>
          parseUploadId(request.uploadId) match {
            case Failure(ex) =>
              complete(StatusCodes.BadRequest -> ErrorResponse(ex.getMessage))
            case Success(uploadId) =>
              val responseFut = llm.sendMessage(uploadId, request.question)
              onSuccess(responseFut) {
                case Right(result) =>
                  val columnViews = result.columns.map { c => ColumnView(c.name, c.dataType) }
                  val response = ChatResponse(result.sql, columnViews, result.rows)
                  complete(response)
                case Left(error) =>
                  complete(StatusCodes.BadRequest -> ErrorResponse(error))
              }
          }
        }
      }
    }

  private def parseUploadId(raw: String): Try[UUID] =
    Try(UUID.fromString(raw))
}
