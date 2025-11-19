package your.org.upload

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.UUID

object UploadCoordinator {

  // ---- Protocol ----

  sealed trait Command

  final case class UploadXlsx(
    uploadId: UUID,
    originalFileName: String,
    tempFile: Path,
    replyTo: ActorRef[UploadResponse]
  ) extends Command

  sealed trait UploadResponse
  final case class UploadAccepted(uploadId: UUID)              extends UploadResponse
  final case class UploadFailed(uploadId: UUID, reason: String) extends UploadResponse

  // ---- Behavior ----

  def apply(
    parser: ActorRef[XlsxParser.Command],
    storageDir: Path
  ): Behavior[Command] =
    Behaviors.setup { ctx =>
      import XlsxParser._

      // We don't use parse results yet; this sink makes the types work cleanly.
      val parseResultSink: ActorRef[ParseResult] =
        ctx.spawn(Behaviors.ignore[ParseResult], "xlsx-parse-result-sink")

      Behaviors.receiveMessage {
        case UploadXlsx(uploadId, originalFileName, tempFile, replyTo) =>
          if (!originalFileName.toLowerCase.endsWith(".xlsx")) {
            replyTo ! UploadFailed(uploadId, "Only .xlsx files are supported")
            Behaviors.same
          } else {
            try {
              Files.createDirectories(storageDir)
              val targetPath = storageDir.resolve(s"$uploadId.xlsx")

              Files.move(tempFile, targetPath, StandardCopyOption.REPLACE_EXISTING)

              // Kick off parsing; results are currently ignored.
              parser ! ParseFile(uploadId, targetPath, replyTo = parseResultSink)

              replyTo ! UploadAccepted(uploadId)
            } catch {
              case ex: Throwable =>
                ctx.log.error(
                  "Upload {} failed while moving file: {}",
                  uploadId,
                  ex.getMessage
                )
                replyTo ! UploadFailed(uploadId, "Internal error while storing file")
            }

            Behaviors.same
          }
      }
    }
}

