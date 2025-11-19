package talksheet.ai.app

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import talksheet.ai.query.{QueryPlanner, WorkbookCatalog}
import talksheet.ai.query.QueryPlanner.{PlanFailed, PlanQuery, PlanSucceeded}
import talksheet.ai.query.SqlExecutor
import talksheet.ai.query.SqlExecutor.{ExecuteQuery, QueryFailed, QueryResult}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.blocking
import scala.concurrent.duration._

object LocalLlm {
  final case class ChatResult(
    sql: String,
    columns: Seq[QueryPlanner.ColumnMeta],
    rows: Seq[Map[String, String]]
  )
}

class LocalLlm(
  planner: ActorRef[QueryPlanner.Command],
  executor: ActorRef[SqlExecutor.Command],
  openAiPlanner: Option[OpenAiQueryPlanner] = None
)(implicit system: ActorSystem[?], timeout: Timeout, ec: ExecutionContext) {

  import LocalLlm._

  def sendMessage(uploadId: UUID, question: String): Future[Either[String, ChatResult]] = {
    waitForWorkbook(uploadId).flatMap {
      case Right(_) =>
        plan(uploadId, question).flatMap {
          case Right((sql, columns)) =>
            val execFut = executor.ask[SqlExecutor.Response](reply => ExecuteQuery(uploadId, sql, reply))(timeout, system.scheduler)
            execFut.map {
              case QueryResult(_, _, rows) => Right(ChatResult(sql, columns, rows))
              case QueryFailed(_, reason)  => Left(reason)
            }
          case Left(error) => Future.successful(Left(error))
        }
      case Left(error) => Future.successful(Left(error))
    }
  }

  private val workbookReadyTimeout = 5.seconds
  private val workbookPollInterval = 50.millis

  private def waitForWorkbook(uploadId: UUID): Future[Either[String, Unit]] = {
    WorkbookCatalog.lookup(uploadId) match {
      case Some(_) => Future.successful(Right(()))
      case None =>
        WorkbookCatalog.status(uploadId) match {
          case Some(WorkbookCatalog.Pending) => pollForWorkbook(uploadId)
          case Some(WorkbookCatalog.Failed(reason)) => Future.successful(Left(reason))
          case None => Future.successful(Left(s"No workbook found for $uploadId"))
        }
    }
  }

  private def pollForWorkbook(uploadId: UUID): Future[Either[String, Unit]] = {
    Future {
      blocking {
        val deadline = workbookReadyTimeout.fromNow
        var ready    = WorkbookCatalog.lookup(uploadId).isDefined
        var status   = WorkbookCatalog.status(uploadId)
        while (!ready && status.contains(WorkbookCatalog.Pending) && deadline.hasTimeLeft()) {
          Thread.sleep(workbookPollInterval.toMillis)
          ready = WorkbookCatalog.lookup(uploadId).isDefined
          status = WorkbookCatalog.status(uploadId)
        }
        if (ready) Right(())
        else {
          status match {
            case Some(WorkbookCatalog.Failed(reason)) => Left(reason)
            case Some(WorkbookCatalog.Pending)        => Left("Workbook is still being processed. Please retry in a moment.")
            case None                                 => Left(s"No workbook found for $uploadId")
          }
        }
      }
    }
  }

  private def plan(uploadId: UUID, question: String): Future[Either[String, (String, Seq[QueryPlanner.ColumnMeta])]] = {
    openAiPlanner match {
      case Some(planner) =>
        planner.plan(uploadId, question).flatMap {
          case Right(result) => Future.successful(Right(result.sql -> result.columns))
          case Left(error) =>
            system.log.warn("OpenAI planner failed, falling back to heuristic planner: {}", error)
            planWithActor(uploadId, question)
        }
      case None => planWithActor(uploadId, question)
    }
  }

  private def planWithActor(uploadId: UUID, question: String) = {
    planner
      .ask[QueryPlanner.Response](reply => PlanQuery(uploadId, question, reply))(timeout, system.scheduler)
      .map {
        case PlanSucceeded(_, sql, columns) => Right(sql -> columns)
        case PlanFailed(_, reason)          => Left(reason)
      }
  }
}
