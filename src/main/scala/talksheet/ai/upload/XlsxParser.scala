package talksheet.ai.upload

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import java.io.FileInputStream
import java.nio.file.Path
import java.util.UUID

object XlsxParser {

  // ---- Protocol ----

  sealed trait Command

  final case class ParseFile(
    uploadId: UUID,
    filePath: Path,
    replyTo: ActorRef[ParseResult]
  ) extends Command

  sealed trait ParseResult
  final case class ParseSuccess(
    uploadId: UUID,
    schema: XlsxSchemaSummary,
    filePath: Path
  ) extends ParseResult
  final case class ParseFailure(uploadId: UUID, reason: String)            extends ParseResult

  final case class XlsxSchemaSummary(sheets: Seq[SheetInfo])
  final case class SheetInfo(
    name: String,
    columns: Seq[String],
    rowCount: Int
  )

  // ---- Behavior ----

  def apply(): Behavior[Command] =
    Behaviors.setup { ctx =>
      Behaviors.receiveMessage {
        case ParseFile(uploadId, filePath, replyTo) =>
          var fis: FileInputStream   = null
          var workbook: XSSFWorkbook = null

          try {
            fis = new FileInputStream(filePath.toFile)
            workbook = new XSSFWorkbook(fis)

            val sheetInfos =
              (0 until workbook.getNumberOfSheets).map { idx =>
                val sheet = workbook.getSheetAt(idx)
                val name  = sheet.getSheetName

                val headerRow = sheet.getRow(0)
                val columns =
                  if (headerRow != null)
                    (0 until headerRow.getLastCellNum.toInt).map { cIdx =>
                      val cell = headerRow.getCell(cIdx)
                      if (cell != null) cell.toString else s"col_$cIdx"
                    }
                  else Seq.empty[String]

                val rowCount = math.max(sheet.getPhysicalNumberOfRows - 1, 0)

                SheetInfo(name, columns, rowCount)
              }

            val summary = XlsxSchemaSummary(sheetInfos)
            ctx.log.info(
              "Parsed XLSX upload {} with {} sheets",
              uploadId,
              sheetInfos.size: java.lang.Integer
            )

            replyTo ! ParseSuccess(uploadId, summary, filePath)
          } catch {
            case ex: Throwable =>
              ctx.log.error("Failed to parse XLSX {}: {}", uploadId, ex.getMessage)
              replyTo ! ParseFailure(uploadId, "Unable to parse XLSX file")
          } finally {
            if (workbook != null) workbook.close()
            if (fis != null)      fis.close()
          }

          Behaviors.same
      }
    }
}
