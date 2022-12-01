// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.scaladsl

import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import cats.effect._
import cats.implicits._
import cats.effect.unsafe.IORuntime
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.projection._
import com.typesafe.scalalogging.StrictLogging
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import io.circe.parser.parse
import io.circe.Json
import org.postgresql.util.PGobject

object Doobie {
  type ActionResult[R] = ConnectionIO[R]
  type Action = ActionResult[Int]

  final case class AdvanceProjectionFailed(projectionId: ProjectionId, offset: Offset)
      extends Exception(
        s"Failed to advance projection ${projectionId} to offset ${offset}".stripMargin
      )
      with scala.util.control.NoStackTrace

  object InitProjection extends StrictLogging {
    def apply(projection: Projection[_])(implicit sys: ActorSystem): Action = {
      for {
        _ <- doobie.free.connection.raw(Migration.migrateIfConfigured)
        u <- insertIfMissing(Migration.projectionTableName, projection)
        _ <- doobie.free.connection.commit
      } yield u
    }

    private def createSql(
        projectionTableName: String,
        projectionId: ProjectionId,
        table: ProjectionTable,
        transactionFilter: TransactionFilter,
        projectionType: String
    ): Fragment = {
      import io.circe.syntax._
      import io.circe.generic.auto._
      val data = transactionFilter.asJson

      sql"""
      | insert into ${Fragment.const(projectionTableName)}(
      |   id,
      |   projection_table,
      |   data,
      |   projection_type,
      |   projection_offset
      | )
      | values (
      |   ${projectionId},
      |   ${table},
      |   ${data},
      |   ${projectionType},
      |   NULL
      | )
      """.stripMargin
    }

    private def insertIfMissing(projectionTableName: String, projection: Projection[_]): Action = {
      import doobie.postgres.sqlstate.class23.UNIQUE_VIOLATION
      createSql(
        projectionTableName,
        projection.id,
        projection.table,
        projection.transactionFilter,
        projection.getClass.getName
      ).update.run.exceptSomeSqlState { case UNIQUE_VIOLATION =>
        doobie.free.connection.pure(0)
      }
    }
  }

  object AdvanceProjection {
    def apply(projectionId: ProjectionId, offset: Offset)(implicit sys: ActorSystem): Action = {
      for {
        u <- update(Migration.projectionTableName, projectionId, offset)
        _ <- if (u <= 0) doobie.free.connection.raiseError(AdvanceProjectionFailed(projectionId, offset))
        else doobie.free.connection.commit
      } yield u
    }
    def update(projectionTableName: String, projectionId: ProjectionId, offset: Offset): Action = {
      sql"""
      | update ${Fragment.const(projectionTableName)}
      |    set projection_offset = ${offset}
      |  where id = ${projectionId}
      """.stripMargin.update.run
    }
  }

  implicit val jsonMeta: Meta[Json] =
    Meta.Advanced
      .other[PGobject]("data")
      .timap[Json](a => parse(a.getValue).left.map[Json](e => throw e).merge)(a => {
        val o = new PGobject
        o.setType("json")
        o.setValue(a.noSpaces)
        o
      })

  private final case class ProjectionData(
      id: ProjectionId,
      table: ProjectionTable,
      data: Json,
      offset: Option[Offset]
  )

  implicit class DoobieProjectionTable(table: ProjectionTable) {
    def const = Fragment.const(table.name)
  }

  object Projector {
    def apply()(implicit xa: Transactor[IO], runtime: IORuntime, sys: ActorSystem): Projector[Action] = {
      new DoobieProjector(xa)
    }

    /*
     * TODO(daml/15691) improve connection usage in Doobie tests https://github.com/digital-asset/daml/issues/15691
     */
    private final class DoobieProjector(xa: Transactor[IO])(implicit runtime: IORuntime, sys: ActorSystem)
        extends Projector[Action]
        with StrictLogging {
      val init: Projection.Init[Action] = Doobie.InitProjection(_)
      val advance: Projection.Advance[Action] = Doobie.AdvanceProjection(_, _)

      def getOffset(projection: Projection[_]): Option[Offset] = {
        (for {
          _ <- doobie.free.connection.raw(Migration.migrateIfConfigured)
          offset <- sql"""
                         | select projection_offset
                         |   from ${Fragment.const(Migration.projectionTableName)}
                         |  where id = ${projection.id}
        """.stripMargin.query[Offset].option
        } yield offset)
          .transact(xa).unsafeRunSync()
      }

      def flow: Flow[Action, Int, ProjectorResource] = {
        import akka.Done
        import scala.concurrent.Future
        val r = xa.rawTrans
        def transactRaw[O](io: ConnectionIO[O]) = r.apply(io).unsafeRunSync()
        Flow[Action]
          .map(transactRaw)
          .withAttributes(com.daml.projection.scaladsl.Projector.blockingDispatcherAttrs)
          .mapMaterializedValue(_ =>
            new ProjectorResource() {
              def cancel() = Future.successful(Done)
              def closed = Future.successful(Done)
            })
      }
    }
  }

  object Copy {
    def apply[R: doobie.postgres.Text](sqlF: Fragment): Seq[R] => Action = {
      l => sqlF.copyIn(l).map(_.toInt)
    }
  }

  object UpdateMany {
    def apply[R: doobie.Write](sql: String): Seq[R] => Action = {
      val updater = Update[R](sql, None)
      l => updater.updateMany(l)
    }
  }
}
