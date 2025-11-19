package talksheet.ai.upload

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.apache.poi.ss.usermodel.{Cell, CellType}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.sqlite.SQLiteDataSource
import talksheet.ai.query.WorkbookCatalog
import talksheet.ai.query.WorkbookCatalog.{Entry, SheetMetadata}
import talksheet.ai.upload.XlsxParser.{ParseFailure, ParseResult, ParseSuccess}

import java.io.FileInputStream
import java.sql.Connection
import java.util.UUID
import scala.util.Using

object WorkbookLoader {

  def apply(): Behavior[ParseResult] =
    Behaviors.setup { ctx =>
      Behaviors.receiveMessage {
        case ParseSuccess(uploadId, schema, filePath) =>
          ctx.log.info("Loading workbook {} into SQLite", uploadId)
          val dataSource = new SQLiteDataSource()
          dataSource.setUrl("jdbc:sqlite::memory:")

          var connection: Connection = null

          try {
            connection = dataSource.getConnection
            connection.setReadOnly(false)
            val sheets = loadWorkbookIntoSqlite(uploadId, filePath.toString, connection)
            connection.setReadOnly(true)
            WorkbookCatalog.register(Entry(uploadId, connection, schema, sheets))
            ctx.log.info(
              "Workbook {} ready with {} sheets",
              uploadId,
              Int.box(sheets.size)
            )
          } catch {
            case ex: Throwable =>
              val message = errorMessage(ex)
              ctx.log.error("Failed to load workbook {} into SQLite: {}", uploadId, message)
              if (connection != null) connection.close()
              WorkbookCatalog.markFailed(uploadId, s"Failed to load workbook: $message")
          }

          Behaviors.same

        case ParseFailure(uploadId, reason) =>
          ctx.log.warn("Skipping SQLite load for upload {} due to parser failure: {}", uploadId, reason)
          WorkbookCatalog.markFailed(uploadId, reason)
          Behaviors.same
      }
    }

  private def loadWorkbookIntoSqlite(
    uploadId: UUID,
    filePath: String,
    connection: Connection
  ): Seq[SheetMetadata] = {
    Using.resource(new FileInputStream(filePath)) { fis =>
      Using.resource(new XSSFWorkbook(fis)) { workbook =>
        (0 until workbook.getNumberOfSheets).flatMap { idx =>
          val sheet = workbook.getSheetAt(idx)
          Option(sheet).map { s =>
            val tableName = sanitizeIdentifier(s"${uploadId}_${s.getSheetName}")
            val headerRow = Option(s.getRow(0)).getOrElse(throw new IllegalStateException("Missing header row"))
            val columnNames = dedupeColumns((0 until headerRow.getLastCellNum.toInt).map { cIdx =>
              val raw = Option(headerRow.getCell(cIdx)).map(_.toString).getOrElse(s"col_$cIdx")
              sanitizeIdentifier(raw)
            })

            createTable(connection, tableName, columnNames)
            insertRows(connection, tableName, columnNames, s)

            SheetMetadata(s.getSheetName, tableName, columnNames)
          }
        }
      }
    }
  }

  private def createTable(connection: Connection, tableName: String, columns: Seq[String]): Unit = {
    val ddl = s"CREATE TABLE IF NOT EXISTS \"$tableName\" (${columns.map(c => s"\"$c\" TEXT").mkString(", ")})"
    val stmt = connection.createStatement()
    try stmt.executeUpdate(ddl)
    finally stmt.close()
  }

  private def insertRows(
    connection: Connection,
    tableName: String,
    columns: Seq[String],
    sheet: org.apache.poi.ss.usermodel.Sheet
  ): Unit = {
    val insertSql =
      s"INSERT INTO \"$tableName\" (${columns.map(c => s"\"$c\"").mkString(", ")}) VALUES (${columns.map(_ => "?").mkString(", ")})"
    val pstmt = connection.prepareStatement(insertSql)

    try {
      (1 to sheet.getLastRowNum).foreach { rowIdx =>
        val row = sheet.getRow(rowIdx)
        if (row != null) {
          columns.indices.foreach { cIdx =>
            val cellValue = cellToString(Option(row.getCell(cIdx)))
            pstmt.setString(cIdx + 1, cellValue.orNull)
          }
          pstmt.addBatch()
        }
      }
      pstmt.executeBatch()
    } finally {
      pstmt.close()
    }
  }

  private def cellToString(cellOpt: Option[Cell]): Option[String] =
    cellOpt.map { cell =>
      cell.getCellType match {
        case CellType.NUMERIC =>
          val numeric = cell.getNumericCellValue
          if (numeric.isValidInt && numeric == numeric.toInt) numeric.toInt.toString
          else numeric.toString
        case CellType.BOOLEAN => cell.getBooleanCellValue.toString
        case CellType.STRING  => cell.getStringCellValue
        case CellType.FORMULA => Option(cell.getStringCellValue).getOrElse(cell.getNumericCellValue.toString)
        case _                => Option(cell.toString).getOrElse("")
      }
    }.map(_.trim).filter(_.nonEmpty)

  private def sanitizeIdentifier(raw: String): String = {
    val candidate = raw.trim.replaceAll("[^A-Za-z0-9_]", "_")
    if (candidate.isEmpty) "col" else candidate
  }

  private def dedupeColumns(columns: Seq[String]): Seq[String] = {
    val seen = scala.collection.mutable.Map.empty[String, Int]
    columns.map { col =>
      val count = seen.getOrElse(col, 0)
      seen.update(col, count + 1)
      if (count == 0) col else s"${col}_$count"
    }
  }

  private def errorMessage(ex: Throwable): String =
    Option(ex.getMessage).filter(_.nonEmpty).getOrElse(ex.getClass.getSimpleName)
}
