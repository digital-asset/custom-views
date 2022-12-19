// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.FlywayException

import javax.sql.DataSource
import scala.jdk.CollectionConverters._

private object Migration extends StrictLogging {
  def projectionTableName(implicit sys: ActorSystem) =
    sys.settings.config.getString("projection.projection-table-name")

  def migrateIfConfigured(ds: DataSource)(implicit sys: ActorSystem): Int = {
    val migrateOnStart = sys.settings.config.getBoolean("projection.flyway.migrate-on-start")
    logger.debug(s"flyway migrate-on-start: $migrateOnStart")
    val internalLocations = sys.settings.config.getStringList("projection.flyway.internal-locations").asScala.toList
    logger.debug(s"flyway internal-locations: $internalLocations")
    val userProvidedLocations = sys.settings.config.getStringList("projection.flyway.locations").asScala.toList
    logger.debug(s"flyway user provided locations: ${userProvidedLocations.mkString(", ")}")

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
      logger.debug(s"Migrating on start, validating flyway.")
      val validationResult = flyway.validateWithResult()

      if (validationResult.validationSuccessful) {
        logger.debug(s"Validated flyway, no need for migration.")
      } else {
        logger.debug(s"Flyway validation failed: ${validationResult.getAllErrorMessages}")
        logger.debug(s"Attempting Flyway migration.")
        val result = try {
          flyway.migrate()
        } catch {
          case e: FlywayException =>
            logger.error("Flyway migration failed", e)
            throw e
        }
        if (!result.warnings.isEmpty) {
          logger.warn(s"Flyway warnings: ${result.warnings.asScala.mkString(", ")}")
        }
        if (result.success) {
          logger.debug(s"Flyway executed ${result.migrationsExecuted} successfully in schema: ${result.schemaName}")
        } else {
          logger.warn(s"Flyway migration failed: ${result.warnings.asScala.mkString(", ")}")
        }
      }
    }
    0
  }
}
