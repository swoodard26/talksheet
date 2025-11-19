package talksheet.ai.app

import talksheet.ai.query.QueryPlanner.ColumnMeta
import talksheet.ai.query.{QueryPlanner, WorkbookCatalog}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object OpenAiQueryPlanner {
  final case class Plan(sql: String, columns: Seq[ColumnMeta])
}

class OpenAiQueryPlanner(client: OpenAiClient)(implicit ec: ExecutionContext) {

  import OpenAiQueryPlanner._
  import OpenAiClient.SqlPlan

  def plan(uploadId: UUID, question: String): Future[Either[String, Plan]] = {
    WorkbookCatalog.lookup(uploadId) match {
      case None => Future.successful(Left(s"No workbook catalog entry found for $uploadId"))
      case Some(entry) =>
        client.generateSqlPlan(entry, question).map {
          case Left(error) => Left(error)
          case Right(plan) =>
            entry.sheets.find(_.tableName == plan.tableName) match {
              case None => Left(s"Model referenced unknown table '${plan.tableName}'")
              case Some(sheet) =>
                val columnMetas = columnsFromPlan(plan, sheet.columns)
                Right(Plan(plan.sqlQuery, columnMetas))
            }
        }
    }
  }

  private def columnsFromPlan(plan: SqlPlan, fallback: Seq[String]): Seq[ColumnMeta] = {
    val names = if (plan.columns.nonEmpty) plan.columns else fallback
    names.map(name => ColumnMeta(name, "TEXT"))
  }
}
