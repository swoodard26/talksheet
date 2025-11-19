package talksheet.ai.app

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import talksheet.ai.query.QueryPlanner
import talksheet.ai.query.QueryPlanner.{ColumnMeta, PlanFailed, PlanQuery, PlanSucceeded}
import talksheet.ai.query.SqlExecutor
import talksheet.ai.query.SqlExecutor.{ExecuteQuery, QueryFailed, QueryResult}

import java.util.UUID
import scala.concurrent.duration._

class LocalLlmSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  private val testKit = ActorTestKit()

  implicit private val system = testKit.system
  implicit private val ec     = system.executionContext
  implicit private val timeout: Timeout = 1.second

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(2.seconds, 25.millis)

  override def afterAll(): Unit =
    testKit.shutdownTestKit()

  "LocalLlm" should {
    "plan a query and return SQL rows" in {
      val plannerBehavior = Behaviors.receiveMessage[QueryPlanner.Command] {
        case PlanQuery(uploadId, question, replyTo) =>
          replyTo ! PlanSucceeded(uploadId, s"SELECT 1 -- $question", Seq(ColumnMeta("col", "TEXT")))
          Behaviors.same
      }
      val executorBehavior = Behaviors.receiveMessage[SqlExecutor.Command] {
        case ExecuteQuery(uploadId, sql, replyTo) =>
          replyTo ! QueryResult(uploadId, Seq("col"), Seq(Map("col" -> sql)))
          Behaviors.same
      }

      val planner  = testKit.spawn(plannerBehavior)
      val executor = testKit.spawn(executorBehavior)
      val llm      = new LocalLlm(planner, executor)

      val uploadId = UUID.randomUUID()
      val result   = llm.sendMessage(uploadId, "hello world").futureValue

      result shouldBe Right(LocalLlm.ChatResult("SELECT 1 -- hello world", Seq(ColumnMeta("col", "TEXT")), Seq(Map("col" -> "SELECT 1 -- hello world"))))
    }

    "propagate planning failures" in {
      val plannerBehavior = Behaviors.receiveMessage[QueryPlanner.Command] {
        case PlanQuery(uploadId, _, replyTo) =>
          replyTo ! PlanFailed(uploadId, "missing workbook")
          Behaviors.same
      }
      val executor = testKit.spawn(Behaviors.ignore[SqlExecutor.Command])
      val llm      = new LocalLlm(testKit.spawn(plannerBehavior), executor)

      val uploadId = UUID.randomUUID()
      val result   = llm.sendMessage(uploadId, "anything").futureValue

      result shouldBe Left("missing workbook")
    }

    "propagate SQL execution failures" in {
      val plannerBehavior = Behaviors.receiveMessage[QueryPlanner.Command] {
        case PlanQuery(uploadId, _, replyTo) =>
          replyTo ! PlanSucceeded(uploadId, "SELECT 1", Seq(ColumnMeta("col", "TEXT")))
          Behaviors.same
      }
      val executorBehavior = Behaviors.receiveMessage[SqlExecutor.Command] {
        case ExecuteQuery(uploadId, _, replyTo) =>
          replyTo ! QueryFailed(uploadId, "boom")
          Behaviors.same
      }

      val llm = new LocalLlm(testKit.spawn(plannerBehavior), testKit.spawn(executorBehavior))

      val result = llm.sendMessage(UUID.randomUUID(), "question").futureValue

      result shouldBe Left("boom")
    }
  }
}
