// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import org.flywaydb.core.Flyway

import javax.sql.DataSource
import scala.jdk.CollectionConverters._
import scala.util.Try

private object Migration extends StrictLogging {
  def projectionTableName(implicit sys: ActorSystem) =
    sys.settings.config.getString("projection.projection-table-name")

  def migrateIfConfigured(ds: DataSource)(implicit sys: ActorSystem): Int = {
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
      .dataSource(ds)
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
        }.recover(e => logger.error(s"Flyway migration failed.", e))
        ()
      }
    }
    0
  }
}
