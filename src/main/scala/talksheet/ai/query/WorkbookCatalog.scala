package talksheet.ai.query

import talksheet.ai.upload.XlsxParser.XlsxSchemaSummary

import java.sql.Connection
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

object WorkbookCatalog {

  final case class SheetMetadata(
    originalName: String,
    tableName: String,
    columns: Seq[String]
  )

  final case class Entry(
    uploadId: UUID,
    connection: Connection,
    schema: XlsxSchemaSummary,
    sheets: Seq[SheetMetadata]
  )

  private val entries = new ConcurrentHashMap[UUID, Entry]()

  def register(entry: Entry): Unit =
    entries.put(entry.uploadId, entry)

  def lookup(uploadId: UUID): Option[Entry] =
    Option(entries.get(uploadId))

  def unregister(uploadId: UUID): Unit =
    entries.remove(uploadId)

  def clear(): Unit =
    entries.clear()
}
