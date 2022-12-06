/*
 * Copied and modified:
 * https://github.com/softwaremill/bootzooka/blob/master/backend/src/test/scala/com/softwaremill/bootzooka/test/TestDB.scala
 * Apache 2.0 Licensed.
 */
package com.daml.projection
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie._
import doobie.implicits._
import com.typesafe.scalalogging.StrictLogging
import com.zaxxer.hikari.{ HikariConfig, HikariDataSource }
import org.flywaydb.core.Flyway

import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try

// TODO separate Doobie and Jdbc?
class TestDB(config: DBConfig) extends StrictLogging {
  val props = new java.util.Properties()
  props.setProperty("user", config.username)
  props.setProperty("password", config.password.value)
  // TODO
  val poolSize = 200
  val connections = new AtomicReference(Vector.empty[java.sql.Connection])
  val hikariConfig = new HikariConfig()
  hikariConfig.setJdbcUrl(config.url)
  hikariConfig.setUsername(config.username)
  hikariConfig.setAutoCommit(false)
  hikariConfig.setPassword(config.password.value)
  hikariConfig.setMaximumPoolSize(poolSize)
  // TODO close ds
  val ds = new HikariDataSource(hikariConfig)
  def getConnection() = {
    val con = ds.getConnection()
    con.setAutoCommit(false)
    connections.getAndUpdate(_ :+ con)
    con
  }

  def xa(implicit ec: ExecutionContext): Transactor.Aux[IO, _ <: DataSource] = {
    Transactor
      .fromDataSource[IO](
        ds,
        connectEC = ec
      )
  }

  private val flyway = {
    Flyway
      .configure()
      .dataSource(config.url, config.username, config.password.value)
      .locations("db.migration.projection", "testdb/migration")
      .load()
  }

  def clean(): Unit = {
    logger.trace("Cleaning database...")
    connections.get().foreach { c =>
      if (!c.isClosed) {
        logger.trace(s"Attempting rollback ${c}")
        Try(c.rollback()).recover(t => logger.trace("Could not rollback connection.", t))
      }
    }
    flyway.clean()
    logger.trace("Database cleaned.")
    ()
  }

  def testConnection(): Unit = {
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    sql"select 1".query[Int].unique.transact(xa).unsafeRunTimed(1.minute)
    ()
  }

  def close(): Unit = {
    connections.get().foreach { c =>
      if (!c.isClosed) {
        logger.trace(s"Closing ${c}")
        Try(c.close()).recover(t => logger.trace("Could not close connection.", t))
      }
    }
  }
}
