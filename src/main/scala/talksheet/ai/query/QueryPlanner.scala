package talksheet.ai.query

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

import java.util.UUID

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
          WorkbookCatalog.lookup(uploadId) match {
            case Some(entry) =>
              entry.sheets.headOption match {
                case Some(sheet) =>
                  val sql = s"""SELECT * FROM "${sheet.tableName}" LIMIT 20"""
                  val columns = sheet.columns.map(col => ColumnMeta(col, "TEXT"))
                  replyTo ! PlanSucceeded(uploadId, sql, columns)
                case None =>
                  replyTo ! PlanFailed(uploadId, "Workbook contains no sheets to query")
              }
            case None =>
              replyTo ! PlanFailed(uploadId, s"No workbook catalog entry found for $uploadId")
          }
          Behaviors.same
      }
    }
}
