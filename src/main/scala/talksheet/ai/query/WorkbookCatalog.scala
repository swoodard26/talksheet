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

  sealed trait UploadStatus
  case object Pending                                    extends UploadStatus
  final case class Failed(reason: String)               extends UploadStatus

  private val entries  = new ConcurrentHashMap[UUID, Entry]()
  private val statuses = new ConcurrentHashMap[UUID, UploadStatus]()

  def register(entry: Entry): Unit = {
    entries.put(entry.uploadId, entry)
    statuses.remove(entry.uploadId)
  }

  def lookup(uploadId: UUID): Option[Entry] =
    Option(entries.get(uploadId))

  def unregister(uploadId: UUID): Unit = {
    entries.remove(uploadId)
    statuses.remove(uploadId)
  }

  def clear(): Unit = {
    entries.clear()
    statuses.clear()
  }

  def markProcessing(uploadId: UUID): Unit =
    statuses.put(uploadId, Pending)

  def markFailed(uploadId: UUID, reason: String): Unit =
    statuses.put(uploadId, Failed(reason))

  def status(uploadId: UUID): Option[UploadStatus] =
    Option(statuses.get(uploadId))
}
