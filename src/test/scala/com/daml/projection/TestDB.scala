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
import org.flywaydb.core.Flyway

import java.util.concurrent.atomic.AtomicReference
import java.sql.DriverManager
import scala.concurrent.duration._
import scala.util.Try

class TestDB(config: DBConfig) extends StrictLogging {
  val props = new java.util.Properties()
  props.setProperty("user", config.username)
  props.setProperty("password", config.password.value)

  val connections = new AtomicReference(Vector.empty[java.sql.Connection])

  def getConnection() = {
    DriverManager.getDrivers()
    val con = DriverManager.getConnection(config.url, props)
    con.setAutoCommit(false)
    connections.getAndUpdate(_ :+ con)
    con
  }
  def xa: Transactor[IO] = {
    val con = getConnection()
    Transactor.fromConnection(con)
  }

  private val flyway = {
    Flyway
      .configure()
      .dataSource(config.url, config.username, config.password.value)
      .locations("db/migration/projection", "testdb/migration")
      .load()
  }

  def migrate(): Unit = {
    if (config.migrateOnStart) {
      flyway.migrate()
      ()
    }
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
