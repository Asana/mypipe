package mypipe.mysql

import mypipe.api.data.{ ColumnMetadata, Table, PrimaryKey }
import mypipe.api.event.TableMapEvent
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ Future, Await }
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

/** A cache for tables whose metadata needs to be looked up against
 *  the database in order to determine column and key structure.
 *
 *  @param hostname of the database
 *  @param port of the database
 *  @param username used to authenticate against the database
 *  @param password used to authenticate against the database
 */
class TableCache(hostname: String, port: Int, username: String, password: String) {
  protected val system = ActorSystem("mypipe")
  protected implicit val ec = system.dispatcher
  protected val dbMetadata = system.actorOf(MySQLMetadataManager.props(hostname, port, username, Some(password)), s"DBMetadataActor-$hostname:$port")
  protected val log = LoggerFactory.getLogger(getClass)

  case class TableKey(database: String, tableName: String)
  protected val tablesByKey = scala.collection.mutable.HashMap[TableKey, Table]()
  protected val tableIdToKey = scala.collection.mutable.HashMap[Long, TableKey]()

  def getTable(tableId: Long): Option[Table] = {
    tableIdToKey.get(tableId).flatMap(tablesByKey.get(_))
  }

  def getTable(database: String, tableName: String): Option[Table] = {
    tablesByKey.get(TableKey(database, tableName))
  }

  def refreshTable(database: String, table: String): Option[Table] = {
    // FIXME: if the table is not in the map we can't refresh it.
    tablesByKey.get(TableKey(database, table)).flatMap(table => {
      Some(addTable(table.id, table.db, table.name, flushTableCache = true))
    })
  }

  def addTableByEvent(ev: TableMapEvent): Table = {
    addTable(ev.tableId, ev.database, ev.tableName, flushTableCache = false)
  }

  def addTable(tableId: Long, database: String, tableName: String, flushTableCache: Boolean): Table = {
    val key = TableKey(database, tableName)
    if (flushTableCache) {
      tablesByKey.remove(key)
    }
    tableIdToKey.put(tableId, key)
    tablesByKey.getOrElseUpdate(key, {
      val table = lookupTable(tableId, database, tableName)
      table
    })
  }

  private def lookupTable(tableId: Long, database: String, tableName: String): Table = {

    // TODO: make this configurable
    implicit val timeout = Timeout(2.second)

    val future = ask(dbMetadata, GetColumns(database, tableName, flushCache = true)).asInstanceOf[Future[(List[ColumnMetadata], Option[PrimaryKey])]]

    // FIXME: handle timeout
    val columns = Await.result(future, 2.seconds)

    Table(tableId, tableName, database, columns._1, columns._2)
  }
}
