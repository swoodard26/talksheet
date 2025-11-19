package talksheet.ai.app

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import talksheet.ai.query.QueryPlanner
import talksheet.ai.query.QueryPlanner.{PlanFailed, PlanQuery, PlanSucceeded}
import talksheet.ai.query.SqlExecutor
import talksheet.ai.query.SqlExecutor.{ExecuteQuery, QueryFailed, QueryResult}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object LocalLlm {
  final case class ChatResult(
    sql: String,
    columns: Seq[QueryPlanner.ColumnMeta],
    rows: Seq[Map[String, String]]
  )
}

class LocalLlm(
  planner: ActorRef[QueryPlanner.Command],
  executor: ActorRef[SqlExecutor.Command]
)(implicit system: ActorSystem[?], timeout: Timeout, ec: ExecutionContext) {

  import LocalLlm._

  def sendMessage(uploadId: UUID, question: String): Future[Either[String, ChatResult]] = {
    val planFut = planner.ask[QueryPlanner.Response](reply => PlanQuery(uploadId, question, reply))(timeout, system.scheduler)

    planFut.flatMap {
      case PlanSucceeded(_, sql, columns) =>
        val execFut = executor.ask[SqlExecutor.Response](reply => ExecuteQuery(uploadId, sql, reply))(timeout, system.scheduler)
        execFut.map {
          case QueryResult(_, _, rows) => Right(ChatResult(sql, columns, rows))
          case QueryFailed(_, reason)  => Left(reason)
        }
      case PlanFailed(_, reason) =>
        Future.successful(Left(reason))
    }
  }
}
