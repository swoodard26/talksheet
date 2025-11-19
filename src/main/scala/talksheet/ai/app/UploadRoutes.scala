package talksheet.ai.app

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.FileInfo
import akka.stream.{IOResult, Materializer, SystemMaterializer}
import akka.stream.scaladsl.FileIO
import talksheet.ai.upload.UploadCoordinator

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class UploadRoutes(
  uploadCoordinator: ActorRef[UploadCoordinator.Command]
)(implicit
  system: ActorSystem[?],
  ec: ExecutionContext
) {

  implicit private val mat: Materializer =
    SystemMaterializer(system).materializer

  private def createTempXlsxFile(): Path =
    Files.createTempFile("upload_", ".xlsx")

  val route: Route =
    path("upload-xlsx") {
      post {
        // Expect multipart/form-data with field name "file"
        fileUpload("file") {
          case (fileInfo: FileInfo, byteSource) =>
            val uploadId: UUID = UUID.randomUUID()
            val tempFile: Path = createTempXlsxFile()

            val writeFut: Future[IOResult] =
              byteSource.runWith(FileIO.toPath(tempFile))

            onSuccess(writeFut) { ioResult =>
              if (ioResult.wasSuccessful) {
                // Fire-and-forget: tell the actor to process the file
                uploadCoordinator ! UploadCoordinator.UploadXlsx(
                  uploadId = uploadId,
                  originalFileName = fileInfo.fileName,
                  tempFile = tempFile
                )

                complete(
                  StatusCodes.OK,
                  s"""{"uploadId":"$uploadId"}"""
                )
              } else {
                complete(
                  StatusCodes.InternalServerError,
                  """{"error":"Failed to write uploaded file"}"""
                )
              }
            }
        }
      }
    }
}
