// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import org.flywaydb.core.Flyway

import scala.jdk.CollectionConverters._
import scala.util.Try
import java.io.PrintWriter
import java.{ sql, util }
import java.sql.{
  Blob,
  CallableStatement,
  Clob,
  Connection,
  DatabaseMetaData,
  NClob,
  PreparedStatement,
  SQLWarning,
  SQLXML,
  Savepoint,
  Statement,
  Struct
}
import java.util.Properties
import java.util.concurrent.Executor
import java.util.logging.Logger

private object Migration extends StrictLogging {
  def projectionTableName(implicit sys: ActorSystem) =
    sys.settings.config.getString("projection.projection-table-name")

  def migrateIfConfigured(con: java.sql.Connection)(implicit sys: ActorSystem): Int = {
    val migrateOnStart = sys.settings.config.getBoolean("projection.flyway.migrate-on-start")
    val internalLocations = sys.settings.config.getStringList("projection.flyway.internal-locations").asScala.toList
    val userProvidedLocations = sys.settings.config.getStringList("projection.flyway.locations").asScala.toList
    val flywayLocations = internalLocations ++ userProvidedLocations
    val flyway = Flyway.configure()
      .placeholders(
        Map(
          "projection_table_name" -> projectionTableName
        ).asJava
      )
      .dataSource(new SingleConnectionDataSource(con))
      .locations(flywayLocations: _*)
      .load()
    if (migrateOnStart) {
      Try {
        flyway.validate()
      }.recover { e =>
        logger.trace(s"Flyway validation failed: ${e.getMessage}")
        logger.trace(s"Attempting Flyway migration.")
        Try {
          val result = flyway.migrate()
          logger.trace(s"Flyway migration success: ${result.schemaName}")
          con.commit()
        }.recover(e => logger.error(s"Flyway migration failed.", e))
        ()
      }
    }
    0
  }
  // Flyway needs a Datasource, this class provides one connection and does not close it
  // since the connection is already closed by the projection infrastructure.
  private class SingleConnectionDataSource(connection: Connection) extends javax.sql.DataSource {
    override def getConnection: Connection = new NonClosingConnection(connection)

    override def getConnection(username: String, password: String): Connection = ???

    override def getLogWriter: PrintWriter = throw new UnsupportedOperationException("getLogWriter")

    override def setLogWriter(out: PrintWriter): Unit = throw new UnsupportedOperationException("setLogWriter")

    override def setLoginTimeout(seconds: Int): Unit = throw new UnsupportedOperationException("setLoginTimeout")

    override def getLoginTimeout: Int = throw new UnsupportedOperationException("getLoginTimeout")

    override def getParentLogger: Logger = throw new UnsupportedOperationException("getParentLogger")

    override def unwrap[T](iface: Class[T]): T = throw new UnsupportedOperationException("unwrap")

    override def isWrapperFor(iface: Class[_]): Boolean = throw new UnsupportedOperationException("isWrapperFor")

    private class NonClosingConnection(con: Connection) extends Connection {
      override def close(): Unit = {}

      override def createStatement(): Statement = con.createStatement()

      override def prepareStatement(sql: String): PreparedStatement = con.prepareStatement(sql)

      override def prepareCall(sql: String): CallableStatement = con.prepareCall(sql)

      override def nativeSQL(sql: String): String = con.nativeSQL(sql)

      override def setAutoCommit(autoCommit: Boolean): Unit = con.setAutoCommit(autoCommit)

      override def getAutoCommit: Boolean = con.getAutoCommit

      override def commit(): Unit = con.commit()

      override def rollback(): Unit = con.rollback()

      override def isClosed: Boolean = con.isClosed

      override def getMetaData: DatabaseMetaData = con.getMetaData

      override def setReadOnly(readOnly: Boolean): Unit = con.setReadOnly(readOnly)

      override def isReadOnly: Boolean = con.isReadOnly

      override def setCatalog(catalog: String): Unit = con.setCatalog(catalog)

      override def getCatalog: String = con.getCatalog

      override def setTransactionIsolation(level: Int): Unit = con.setTransactionIsolation(level)

      override def getTransactionIsolation: Int = con.getTransactionIsolation

      override def getWarnings: SQLWarning = con.getWarnings

      override def clearWarnings(): Unit = con.clearWarnings()

      override def createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement =
        con.createStatement(resultSetType, resultSetConcurrency)

      override def prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement =
        con.prepareStatement(sql, resultSetType, resultSetConcurrency)

      override def prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement =
        con.prepareCall(sql, resultSetType, resultSetConcurrency)

      override def getTypeMap: util.Map[String, Class[_]] = con.getTypeMap

      override def setTypeMap(map: util.Map[String, Class[_]]): Unit = con.setTypeMap(map)

      override def setHoldability(holdability: Int): Unit = con.setHoldability(holdability)

      override def getHoldability: Int = con.getHoldability

      override def setSavepoint(): Savepoint = con.setSavepoint()

      override def setSavepoint(name: String): Savepoint = con.setSavepoint(name)

      override def rollback(savepoint: Savepoint): Unit = con.rollback()

      override def releaseSavepoint(savepoint: Savepoint): Unit = con.releaseSavepoint(savepoint)

      override def createStatement(
          resultSetType: Int,
          resultSetConcurrency: Int,
          resultSetHoldability: Int): Statement =
        con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)

      override def prepareStatement(
          sql: String,
          resultSetType: Int,
          resultSetConcurrency: Int,
          resultSetHoldability: Int): PreparedStatement =
        con.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)

      override def prepareCall(
          sql: String,
          resultSetType: Int,
          resultSetConcurrency: Int,
          resultSetHoldability: Int): CallableStatement =
        con.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)

      override def prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement =
        con.prepareStatement(sql, autoGeneratedKeys)

      override def prepareStatement(sql: String, columnIndexes: Array[Int]): PreparedStatement =
        con.prepareStatement(sql, columnIndexes)

      override def prepareStatement(sql: String, columnNames: Array[String]): PreparedStatement =
        con.prepareStatement(sql, columnNames)

      override def createClob(): Clob = con.createClob()

      override def createBlob(): Blob = con.createBlob()

      override def createNClob(): NClob = con.createNClob()

      override def createSQLXML(): SQLXML = con.createSQLXML()

      override def isValid(timeout: Int): Boolean = con.isValid(timeout)

      override def setClientInfo(name: String, value: String): Unit = con.setClientInfo(name, value)

      override def setClientInfo(properties: Properties): Unit = con.setClientInfo(properties)

      override def getClientInfo(name: String): String = con.getClientInfo(name)

      override def getClientInfo: Properties = con.getClientInfo

      override def createArrayOf(typeName: String, elements: Array[AnyRef]): sql.Array =
        con.createArrayOf(typeName, elements)

      override def createStruct(typeName: String, attributes: Array[AnyRef]): Struct =
        con.createStruct(typeName, attributes)

      override def setSchema(schema: String): Unit = con.setSchema(schema)

      override def getSchema: String = con.getSchema

      override def abort(executor: Executor): Unit = con.abort(executor)

      override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit =
        con.setNetworkTimeout(executor, milliseconds)

      override def getNetworkTimeout: Int = con.getNetworkTimeout

      override def unwrap[T](iface: Class[T]): T = con.unwrap(iface)

      override def isWrapperFor(iface: Class[_]): Boolean = con.isWrapperFor(iface)
    }
  }

}
