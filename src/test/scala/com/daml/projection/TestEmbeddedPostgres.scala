/*
 * Copied and modified:
 * https://github.com/softwaremill/bootzooka/blob/master/backend/src/test/scala/com/softwaremill/bootzooka/test/TestEmbeddedPostgres.scala
 * Apache 2.0 Licensed.
 */
package com.daml.projection

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, Suite }

case class Sensitive(value: String) extends AnyVal {
  override def toString: String = "***"
}

case class DBConfig(
    username: String,
    password: Sensitive,
    url: String,
    migrateOnStart: Boolean,
    driver: String,
    connectThreadPoolSize: Int)

object DBConfig {
  val Driver = "org.postgresql.Driver"
  val ConnectThreadPoolSize = 32
  def apply(url: String): DBConfig = {
    DBConfig(
      username = "postgres",
      password = Sensitive(""),
      url = url,
      migrateOnStart = true,
      driver = Driver,
      connectThreadPoolSize = ConnectThreadPoolSize)
  }
}

/**
 * Base trait for tests which use the database. The database is cleaned after each test.
 */
trait TestEmbeddedPostgres extends BeforeAndAfterEach with BeforeAndAfterAll with StrictLogging { self: Suite =>
  private var postgres: EmbeddedPostgres = _
  private var currentDbConfig: DBConfig = _
  var currentDb: TestDB = _
  import cats.effect.IO
  implicit def xa: doobie.util.transactor.Transactor[IO] = currentDb.xa
  def getEmbeddedPostgres() = postgres
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    postgres = EmbeddedPostgres.builder()
      // perf test showed warnings on low max_wal_size which is 1GB by default
      .setServerConfig("max_wal_size", "4096")
      .start()
    val url = postgres.getJdbcUrl("postgres")
    currentDbConfig = DBConfig(url)
    currentDb = new TestDB(currentDbConfig)
    currentDb.testConnection()
  }

  override protected def afterAll(): Unit = {
    currentDb.close()
    postgres.close()
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    currentDb.clean()
    super.afterEach()
  }
}
