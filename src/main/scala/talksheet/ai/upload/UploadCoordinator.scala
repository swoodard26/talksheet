package talksheet.ai.upload

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import talksheet.ai.query.WorkbookCatalog

import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.UUID

object UploadCoordinator {

  // ---- Protocol ----

  sealed trait Command

  final case class UploadXlsx(
    uploadId: UUID,
    originalFileName: String,
    tempFile: Path
  ) extends Command

  // ---- Behavior ----

  def apply(
    parser: ActorRef[XlsxParser.Command],
    storageDir: Path
  ): Behavior[Command] =
    Behaviors.setup { ctx =>
      import XlsxParser._

      val parseResultSink: ActorRef[ParseResult] =
        ctx.spawn(WorkbookLoader(), "workbook-loader")

      Behaviors.receiveMessage {
        case UploadXlsx(uploadId, originalFileName, tempFile) =>
          if (!originalFileName.toLowerCase.endsWith(".xlsx")) {
            ctx.log.warn(
              "Rejected upload {}: unsupported file extension for '{}'",
              uploadId,
              originalFileName
            )
            WorkbookCatalog.markFailed(uploadId, "Unsupported file type. Please upload .xlsx files.")
          } else {
            try {
              Files.createDirectories(storageDir)
              val targetPath = storageDir.resolve(s"$uploadId.xlsx")

              Files.move(tempFile, targetPath, StandardCopyOption.REPLACE_EXISTING)

              ctx.log.info(
                "Stored upload {} as {}",
                uploadId,
                targetPath.toAbsolutePath.toString
              )

              // Kick off parsing
              parser ! ParseFile(uploadId, targetPath, replyTo = parseResultSink)
            } catch {
              case ex: Throwable =>
                val message = failureMessage(ex)
                ctx.log.error(
                  "Upload {} failed while moving file: {}",
                  uploadId,
                  message
                )
                WorkbookCatalog.markFailed(uploadId, s"Failed to store uploaded file: $message")
            }
          }

          Behaviors.same
      }
    }
  private def failureMessage(ex: Throwable): String =
    Option(ex.getMessage).filter(_.nonEmpty).getOrElse(ex.getClass.getSimpleName)
}
