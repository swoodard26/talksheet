package talksheet.ai.app

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.http.scaladsl.model.headers.{Authorization, GenericHttpCredentials, RawHeader}
import akka.stream.Materializer
import spray.json._
import talksheet.ai.query.WorkbookCatalog

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object OpenAiClient {
  final case class SqlPlan(tableName: String, sqlQuery: String, columns: Seq[String])
}

class OpenAiClient(
  apiKey: String,
  model: String,
  baseUrl: String = "https://api.openai.com/v1",
  organization: Option[String] = None
)(implicit system: ActorSystem[?], ec: ExecutionContext, mat: Materializer) {

  import OpenAiClient._

  private val http = Http()
  private val endpoint = if (baseUrl.endsWith("/")) baseUrl.dropRight(1) else baseUrl

  private val systemPrompt =
    """You generate safe, read-only SQLite queries for spreadsheet data. """ +
      """Use only the provided table names, wrap identifiers in double quotes, """ +
      """and default to LIMIT 20 when the user does not ask for a specific amount. """ +
      """Avoid aggregations unless explicitly requested and never invent tables."""

  private val responseFormat: JsObject = JsObject(
    "type" -> JsString("json_schema"),
    "json_schema" -> JsObject(
      "name" -> JsString("talksheet_sql_plan"),
      "schema" -> JsObject(
        "type" -> JsString("object"),
        "additionalProperties" -> JsBoolean(false),
        "required" -> JsArray(JsString("table_name"), JsString("sql_query"), JsString("columns")),
        "properties" -> JsObject(
          "table_name" -> JsObject(
            "type" -> JsString("string"),
            "description" -> JsString("The exact table name to query (case-sensitive).")
          ),
          "sql_query" -> JsObject(
            "type" -> JsString("string"),
            "description" -> JsString(
              "A complete SQLite query selecting only the requested columns from the chosen table."
            )
          ),
          "columns" -> JsObject(
            "type" -> JsString("array"),
            "description" -> JsString("Column names in the SELECT projection order."),
            "items" -> JsObject("type" -> JsString("string"))
          )
        )
      )
    )
  )

  def generateSqlPlan(entry: WorkbookCatalog.Entry, question: String): Future[Either[String, SqlPlan]] = {
    val userPrompt =
      s"""Spreadsheet schema (table name → sheet name → columns):\n${describeSchema(entry)}\n\n""" +
        s"""Question: $question\n""" +
        "Return only valid SQL using the provided tables."

    val payload = JsObject(
      "model" -> JsString(model),
      "temperature" -> JsNumber(0),
      "messages" -> JsArray(
        JsObject("role" -> JsString("system"), "content" -> JsString(systemPrompt)),
        JsObject("role" -> JsString("user"), "content" -> JsString(userPrompt))
      ),
      "response_format" -> responseFormat
    )

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = s"$endpoint/chat/completions",
      entity = HttpEntity(ContentTypes.`application/json`, payload.compactPrint)
    ).withHeaders(buildHeaders)

    http
      .singleRequest(request)
      .flatMap { response =>
        response.entity.toStrict(30.seconds).map { strictEntity =>
          val body = strictEntity.data.utf8String
          if (response.status.isSuccess()) parsePlan(body)
          else Left(s"OpenAI request failed with status ${response.status.intValue()}: $body")
        }
      }
      .recover { case NonFatal(ex) => Left(s"OpenAI request failed: ${ex.getMessage}") }
  }

  private def buildHeaders = {
    val base = List(Authorization(GenericHttpCredentials("Bearer", apiKey)))
    organization match {
      case Some(org) => base :+ RawHeader("OpenAI-Organization", org)
      case None      => base
    }
  }

  private def describeSchema(entry: WorkbookCatalog.Entry): String = {
    entry.sheets
      .map { sheet =>
        val columns = sheet.columns.mkString(", ")
        s"\"${sheet.tableName}\" (sheet \"${sheet.originalName}\"): $columns"
      }
      .mkString("\n")
  }

  private def parsePlan(body: String): Either[String, SqlPlan] = {
    Try(body.parseJson.asJsObject) match {
      case Failure(ex) => Left(s"Failed to parse OpenAI response: ${ex.getMessage}")
      case Success(obj) =>
        extractContent(obj).flatMap { content =>
          Try(content.parseJson.asJsObject) match {
            case Failure(ex) => Left(s"Failed to decode plan JSON: ${ex.getMessage}")
            case Success(planObj) =>
              for {
                tableName <- planObj.fields.get("table_name").collect { case JsString(value) => value }.toRight(
                  "Plan missing table_name"
                )
                sqlQuery <- planObj.fields
                  .get("sql_query")
                  .collect { case JsString(value) => value.trim }
                  .filter(_.nonEmpty)
                  .toRight("Plan missing sql_query")
              } yield {
                val columns = planObj.fields
                  .get("columns")
                  .collect { case JsArray(values) => values.collect { case JsString(v) => v } }
                  .getOrElse(Seq.empty)
                SqlPlan(tableName, sqlQuery, columns)
              }
          }
        }
    }
  }

  private def extractContent(obj: JsObject): Either[String, String] = {
    val contentOpt = for {
      choices <- obj.fields.get("choices")
      first <- choices match {
        case JsArray(values) if values.nonEmpty => Some(values.head)
        case _                                  => None
      }
      message <- first.asJsObject.fields.get("message").map(_.asJsObject)
      rawContent <- message.fields.get("content")
    } yield rawContent

    contentOpt match {
      case None => Left("OpenAI response missing assistant content")
      case Some(JsString(value)) => Right(value)
      case Some(JsArray(values)) =>
        val combined = values.collect {
          case JsObject(fields) if fields.get("type").contains(JsString("text")) =>
            fields.get("text").collect { case JsString(text) => text }.getOrElse("")
        }.mkString
        if (combined.trim.nonEmpty) Right(combined) else Left("OpenAI content array was empty")
      case Some(other) => Left(s"Unexpected content payload: $other")
    }
  }
}
