package talksheet.ai.app

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import talksheet.ai.query.{QueryPlanner, WorkbookCatalog}
import talksheet.ai.query.QueryPlanner.{ColumnMeta, PlanFailed, PlanQuery, PlanSucceeded}
import talksheet.ai.query.SqlExecutor
import talksheet.ai.query.SqlExecutor.{ExecuteQuery, QueryFailed, QueryResult}
import talksheet.ai.upload.XlsxParser.{SheetInfo, XlsxSchemaSummary}

import java.sql.{Connection, DriverManager}
import java.util.UUID
import scala.concurrent.duration._

class LocalLlmSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ScalaFutures {

  private val testKit = ActorTestKit()
  private var openConnections = List.empty[Connection]

  implicit private val system = testKit.system
  implicit private val ec     = system.executionContext
  implicit private val timeout: Timeout = 1.second

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(2.seconds, 25.millis)

  override protected def beforeEach(): Unit = {
    WorkbookCatalog.clear()
    openConnections.foreach(_.close())
    openConnections = Nil
  }

  override def afterAll(): Unit = {
    openConnections.foreach(_.close())
    openConnections = Nil
    WorkbookCatalog.clear()
    testKit.shutdownTestKit()
  }

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
      registerWorkbook(uploadId)
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
      registerWorkbook(uploadId)
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

      val uploadId = UUID.randomUUID()
      registerWorkbook(uploadId)
      val result = llm.sendMessage(uploadId, "question").futureValue

      result shouldBe Left("boom")
    }
  }

  private def registerWorkbook(uploadId: UUID): Unit = {
    val connection = DriverManager.getConnection("jdbc:sqlite::memory:")
    openConnections = connection :: openConnections
    val sheets = Seq(WorkbookCatalog.SheetMetadata("Sheet1", "sheet1", Seq("col")))
    val schema = XlsxSchemaSummary(Seq(SheetInfo("Sheet1", Seq("col"), rowCount = 1)))
    WorkbookCatalog.register(WorkbookCatalog.Entry(uploadId, connection, schema, sheets))
  }
}
