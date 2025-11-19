package talksheet.ai.query

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors

import java.sql.ResultSet
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

object SqlExecutor {

  sealed trait Command
  final case class ExecuteQuery(
    uploadId: UUID,
    sql: String,
    replyTo: ActorRef[Response]
  ) extends Command

  sealed trait Response
  final case class QueryResult(
    uploadId: UUID,
    columns: Seq[String],
    rows: Seq[Map[String, String]]
  ) extends Response
  final case class QueryFailed(uploadId: UUID, reason: String) extends Response

  def apply(): Behavior[Command] =
    Behaviors.setup { ctx =>
      Behaviors.receiveMessage {
        case ExecuteQuery(uploadId, sql, replyTo) =>
          WorkbookCatalog.lookup(uploadId) match {
            case Some(entry) =>
              val connection = entry.connection
              connection.setReadOnly(true)
              val stmt = connection.createStatement()
              try {
                stmt.setQueryTimeout(5)
                val rs = stmt.executeQuery(sql)
                try {
                  val (columns, rows) = materializeResult(rs)
                  replyTo ! QueryResult(uploadId, columns, rows)
                } finally {
                  rs.close()
                }
              } catch {
                case ex: Throwable =>
                  ctx.log.error("SQL execution failed for upload {}: {}", uploadId, ex.getMessage)
                  replyTo ! QueryFailed(uploadId, ex.getMessage)
              } finally {
                stmt.close()
              }
            case None =>
              replyTo ! QueryFailed(uploadId, s"No SQLite connection found for $uploadId")
          }
          Behaviors.same
      }
    }

  private def materializeResult(rs: ResultSet): (Seq[String], Seq[Map[String, String]]) = {
    val meta        = rs.getMetaData
    val columnNames = (1 to meta.getColumnCount).map(meta.getColumnLabel)
    val buffer      = ArrayBuffer.empty[Map[String, String]]

    while (rs.next()) {
      val row = columnNames.map { col =>
        val value = Option(rs.getString(col)).getOrElse("")
        col -> value
      }.toMap
      buffer += row
    }

    (columnNames, buffer.toSeq)
  }
}
