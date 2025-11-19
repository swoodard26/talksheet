package talksheet.ai.query

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import talksheet.ai.upload.XlsxParser.XlsxSchemaSummary
import talksheet.ai.query.WorkbookCatalog.{Entry, SheetMetadata}

import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import scala.collection.mutable.ListBuffer

class QueryPlannerSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with Matchers {

  private val openConnections = ListBuffer.empty[Connection]

  override def afterAll(): Unit = {
    openConnections.foreach(_.close())
    super.afterAll()
  }

  "QueryPlanner" should {
    "plan column-specific queries with limits when the sheet is mentioned" in {
      val uploadId = UUID.randomUUID()
      registerWorkbook(uploadId, Seq(
        SheetMetadata("Sales Data", "tbl_sales", Seq("employee", "region", "amount")),
        SheetMetadata("Inventory", "tbl_inventory", Seq("item", "quantity"))
      ))

      val probe   = createTestProbe[QueryPlanner.Response]()
      val planner = spawn(QueryPlanner())

      val question = "Show top 5 sales data region and amount"
      planner ! QueryPlanner.PlanQuery(uploadId, question, probe.ref)

      val response = probe.expectMessageType[QueryPlanner.PlanSucceeded]
      response.sql shouldEqual "SELECT \"region\", \"amount\" FROM \"tbl_sales\" LIMIT 5"
      response.columns.map(_.name) shouldEqual Seq("region", "amount")
    }

    "default to all columns and limit 20 when no hints are provided" in {
      val uploadId = UUID.randomUUID()
      registerWorkbook(uploadId, Seq(
        SheetMetadata("Employees", "tbl_employees", Seq("id", "name", "department"))
      ))

      val probe   = createTestProbe[QueryPlanner.Response]()
      val planner = spawn(QueryPlanner())

      planner ! QueryPlanner.PlanQuery(uploadId, "List the employees", probe.ref)

      val response = probe.expectMessageType[QueryPlanner.PlanSucceeded]
      response.sql shouldEqual "SELECT \"id\", \"name\", \"department\" FROM \"tbl_employees\" LIMIT 20"
      response.columns.map(_.name) shouldEqual Seq("id", "name", "department")
    }
  }

  private def registerWorkbook(uploadId: UUID, sheets: Seq[SheetMetadata]): Unit = {
    val connection = DriverManager.getConnection("jdbc:sqlite::memory:")
    openConnections += connection
    val schema = XlsxSchemaSummary(Seq.empty)
    WorkbookCatalog.register(Entry(uploadId, connection, schema, sheets))
  }
}
