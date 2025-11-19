package talksheet.ai.query

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

import java.util.UUID
import scala.util.Try

object QueryPlanner {

  // ---- Protocol ----
  sealed trait Command

  final case class PlanQuery(
    uploadId: UUID,
    question: String,
    replyTo: ActorRef[Response]
  ) extends Command

  sealed trait Response
  final case class PlanSucceeded(
    uploadId: UUID,
    sql: String,
    columns: Seq[ColumnMeta]
  ) extends Response
  final case class PlanFailed(uploadId: UUID, reason: String) extends Response

  final case class ColumnMeta(name: String, dataType: String)

  def apply(): Behavior[Command] =
    Behaviors.setup { ctx =>
      Behaviors.receiveMessage {
        case PlanQuery(uploadId, question, replyTo) =>
          ctx.log.info("Planning query for upload {} and question '{}'", uploadId, question)
          val normalizedQuestion = normalize(question)
          WorkbookCatalog.lookup(uploadId) match {
            case Some(entry) =>
              chooseSheet(entry, question, normalizedQuestion) match {
                case Some(sheet) =>
                  if (sheet.columns.isEmpty) {
                    replyTo ! PlanFailed(uploadId, s"Sheet '${sheet.originalName}' has no columns to query")
                  } else {
                    val selectedColumns = selectColumns(sheet, normalizedQuestion)
                    val limit           = extractLimit(question)
                    val sql             = buildSql(sheet.tableName, selectedColumns, limit)
                    val columns         = selectedColumns.map(col => ColumnMeta(col, "TEXT"))
                    replyTo ! PlanSucceeded(uploadId, sql, columns)
                  }
                case None =>
                  replyTo ! PlanFailed(uploadId, "Workbook contains no sheets to query")
              }
            case None =>
              replyTo ! PlanFailed(uploadId, s"No workbook catalog entry found for $uploadId")
          }
          Behaviors.same
      }
    }
  private val DefaultLimit = 20

  private val limitPattern = """(?i)\b(?:top|first|show|list|limit)\s+(\d{1,4})\b""".r

  private def extractLimit(question: String): Int =
    limitPattern
      .findFirstMatchIn(question)
      .flatMap(m => Try(m.group(1).toInt).toOption)
      .filter(_ > 0)
      .getOrElse(DefaultLimit)

  private def chooseSheet(
    entry: WorkbookCatalog.Entry,
    rawQuestion: String,
    normalizedQuestion: String
  ): Option[WorkbookCatalog.SheetMetadata] = {
    val questionLower = rawQuestion.toLowerCase
    entry.sheets.find { sheet =>
      val normalizedSheetName = normalize(sheet.originalName)
      val normalizedHit = normalizedSheetName.nonEmpty && normalizedQuestion.contains(normalizedSheetName)
      val tokenHit = sheet.originalName
        .toLowerCase
        .split("\\s+")
        .filter(_.nonEmpty)
        .exists(questionLower.contains)
      normalizedHit || tokenHit
    }.orElse(entry.sheets.headOption)
  }

  private def selectColumns(
    sheet: WorkbookCatalog.SheetMetadata,
    normalizedQuestion: String
  ): Seq[String] = {
    val matches = sheet.columns.filter { column =>
      val normalizedColumn = normalize(column)
      normalizedColumn.nonEmpty && normalizedQuestion.contains(normalizedColumn)
    }
    if (matches.nonEmpty) matches else sheet.columns
  }

  private def buildSql(tableName: String, columns: Seq[String], limit: Int): String = {
    val projection = columns.map(col => s"\"$col\"").mkString(", ")
    s"""SELECT $projection FROM "$tableName" LIMIT $limit"""
  }

  private def normalize(text: String): String =
    text.toLowerCase.replaceAll("[^a-z0-9]", "")
}
