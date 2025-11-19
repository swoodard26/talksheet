package talksheet.ai.query

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import talksheet.ai.query.QueryPlanner.{PlanFailed, PlanQuery, PlanSucceeded}
import talksheet.ai.upload.XlsxParser.{SheetInfo, XlsxSchemaSummary}

import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import scala.collection.mutable.ListBuffer

class QueryPlannerSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private val testKit = ActorTestKit()
  private val openConnections = ListBuffer.empty[Connection]

  override def afterEach(): Unit = {
    openConnections.foreach(_.close())
    openConnections.clear()
    WorkbookCatalog.clear()
  }

  override def afterAll(): Unit =
    testKit.shutdownTestKit()

  "QueryPlanner" should {
    "plan SQL for the sheet referenced in the question" in {
      val uploadId = UUID.randomUUID()
      val sheets = Seq(
        WorkbookCatalog.SheetMetadata("Regional Revenue 2023", "regional_revenue", Seq("Region", "Revenue", "Profit")),
        WorkbookCatalog.SheetMetadata("Inventory", "inventory", Seq("SKU", "Qty"))
      )
      register(uploadId, sheets)

      val planner = testKit.spawn(QueryPlanner())
      val probe   = testKit.createTestProbe[QueryPlanner.Response]()

      planner ! PlanQuery(uploadId, "Show top 5 Region and Profit from the regional revenue sheet", probe.ref)

      val response = probe.expectMessageType[PlanSucceeded]
      response.sql shouldBe "SELECT \"Region\", \"Profit\" FROM \"regional_revenue\" LIMIT 5"
      response.columns.map(_.name) shouldBe Seq("Region", "Profit")
    }

    "default to the first sheet when no name matches" in {
      val uploadId = UUID.randomUUID()
      val sheets = Seq(
        WorkbookCatalog.SheetMetadata("Finance", "finance", Seq("Account", "Balance")),
        WorkbookCatalog.SheetMetadata("Marketing", "marketing", Seq("Channel", "Spend"))
      )
      register(uploadId, sheets)

      val planner = testKit.spawn(QueryPlanner())
      val probe   = testKit.createTestProbe[QueryPlanner.Response]()

      planner ! PlanQuery(uploadId, "list 3 things", probe.ref)

      val response = probe.expectMessageType[PlanSucceeded]
      response.sql shouldBe "SELECT \"Account\", \"Balance\" FROM \"finance\" LIMIT 3"
      response.columns.map(_.name) shouldBe Seq("Account", "Balance")
    }

    "fail when no workbook entry exists" in {
      val uploadId = UUID.randomUUID()
      val planner  = testKit.spawn(QueryPlanner())
      val probe    = testKit.createTestProbe[QueryPlanner.Response]()

      planner ! PlanQuery(uploadId, "any question", probe.ref)

      val response = probe.expectMessageType[PlanFailed]
      response.reason should include(uploadId.toString)
    }
  }

  private def register(uploadId: UUID, sheets: Seq[WorkbookCatalog.SheetMetadata]): Unit = {
    val connection = DriverManager.getConnection("jdbc:sqlite::memory:")
    openConnections += connection
    val sheetInfo = sheets.map(s => SheetInfo(s.originalName, s.columns, rowCount = 10))
    val schema    = XlsxSchemaSummary(sheetInfo)
    WorkbookCatalog.register(WorkbookCatalog.Entry(uploadId, connection, schema, sheets))
  }
}
