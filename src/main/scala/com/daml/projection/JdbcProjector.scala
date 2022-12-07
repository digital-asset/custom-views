// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Sink }
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.{ event => SE }
import com.daml.ledger.javaapi.{ data => J }
import com.daml.projection.Projection.Advance
import com.daml.projection.scaladsl.{ Projector => SProjector, ProjectorResource }
import com.typesafe.scalalogging.StrictLogging

import java.sql.SQLException
import javax.sql.DataSource
import scala.concurrent.Promise
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Supplies a connection A Supplier interface. Supports throwing `Exception` in the apply, which the
 * `java.util.function.Supplier` counterpart does not.
 */
@SerialVersionUID(1L)
@FunctionalInterface
trait ConnectionSupplier extends java.io.Serializable {
  @throws(classOf[SQLException])
  def apply(): java.sql.Connection
}

/**
 * Projects an event of type `E` wrapped in an [[Envelope]] into a list of `A`, which often is an action type. Supports
 * throwing `Exception`s.
 */
@SerialVersionUID(1L)
@FunctionalInterface
trait Project[E, A] extends java.io.Serializable {
  @throws(classOf[Exception])
  def apply(envelope: Envelope[E]): java.util.List[A]
}

/**
 * Batches `R` rows into an action `A`. Supports throwing `Exception`s.
 */
@SerialVersionUID(1L)
@FunctionalInterface
trait BatchRows[R, A] extends java.io.Serializable {
  @throws(classOf[Exception])
  def apply(rows: java.util.List[R]): A
}

/**
 * Creates a [[javadsl.Projector]] or [[scaladsl.Projector]] that executes [[JdbcAction]]s.
 */
object JdbcProjector {

  def apply(
      ds: DataSource
  )(implicit system: ActorSystem): scaladsl.Projector[JdbcAction] = {
    new JdbcProjectorImpl(ds, system)
  }

  /**
   * Creates a JdbcProjector.
   */
  def create(
      ds: DataSource,
      system: ActorSystem): javadsl.Projector[JdbcAction] = {
    new JdbcProjectorImpl(ds, system)
  }

  private final class JdbcProjectorImpl(
      ds: DataSource,
      system: ActorSystem)
      extends javadsl.Projector[JdbcAction] with StrictLogging {
    val advance: Advance[JdbcAction] = AdvanceProjection(_, _)
    val init: Projection.Init[JdbcAction] = InitProjection(_)
    val projectionTableName = Migration.projectionTableName(system)
    private object AdvanceProjection {
      def apply(projectionId: ProjectionId, offset: Offset): JdbcAction = {
        val sql =
          s"""
            | update $projectionTableName
            |    set projection_offset = :offset
            |  where id = :id
          """.stripMargin

        CommittedAction(ExecuteUpdate.create(sql).bind(1, offset.value).bind(2, projectionId.value))
      }
    }

    private object InitProjection {
      def apply(projection: Projection[_]): JdbcAction = {
        val sql =
          s"""
          | insert into $projectionTableName(
          |   id,
          |   projection_offset
          | )
          | values (
          |   ?,
          |   NULL
          | )
          """.stripMargin

        val insertIfNotExists = HandleError(
          ExecuteUpdate
            .create(sql)
            .bind(1, projection.id.value),
          // rollback automatically starts a new tx
          _ => Rollback
        )
        new JdbcAction() {
          def execute(con: java.sql.Connection): Int = {
            Migration.migrateIfConfigured(ds)
            insertIfNotExists.execute(con)
          }
        }
      }
    }

    implicit val sys: ActorSystem = system
    implicit val projector: scaladsl.Projector[JdbcAction] = this
    private def createCon() = {
      val con = ds.getConnection()
      con.setAutoCommit(false)
      con
    }
    override def getOffset(projection: Projection[_]) = {
      val sql = s"""
        | select projection_offset
        |   from $projectionTableName
        |  where id = ?
        """.stripMargin
      val connection = createCon()
      try {
        Migration.migrateIfConfigured(ds)
        val ps = connection.prepareStatement(sql)
        ps.setString(1, projection.id.value)
        try {
          val rs = ps.executeQuery()
          try {
            if (rs.next()) Option(rs.getString(1)).map(v => Offset(v))
            else None
          } finally {
            rs.close()
          }
        } finally {
          ps.close()
        }
      } finally {
        connection.close()
      }
    }

    override def project[E](
        batchSource: javadsl.BatchSource[E],
        p: Projection[E],
        f: Project[E, JdbcAction]): javadsl.Control = {

      val control = Projection.project(batchSource.toScala, p)(e => f(e).asScala)
      new javadsl.GrpcControl(control)
    }

    override def projectEvents(
        batchSource: javadsl.BatchSource[J.Event],
        p: Projection[J.Event],
        fc: Project[J.CreatedEvent, JdbcAction],
        fa: Project[J.ArchivedEvent, JdbcAction]
    ): javadsl.Control = {

      val sBatchSource = new javadsl.BatchSource[SE.Event]() {
        override def src(projection: Projection[Event])(implicit
            sys: ActorSystem): akka.stream.javadsl.Source[Batch[Event], javadsl.Control] = {
          batchSource.src(projection.convert(e => Some(SE.Event.fromJavaProto(e.toProtoEvent))))
        }.map(b => b.map(e => SE.Event.fromJavaProto(e.toProtoEvent)))
      }.toScala

      val control = Projection.project(
        sBatchSource,
        p.convert(e => Some(J.Event.fromProtoEvent(SE.Event.toJavaProto(e)))),
        c => fc(c.map(ce => J.CreatedEvent.fromProto(SE.CreatedEvent.toJavaProto(ce)))).asScala,
        a => fa(a.map(ae => J.ArchivedEvent.fromProto(SE.ArchivedEvent.toJavaProto(ae)))).asScala
      )
      new javadsl.GrpcControl(control)
    }

    import scala.jdk.CollectionConverters._

    override def projectRows[E, R](
        batchSource: javadsl.BatchSource[E],
        p: Projection[E],
        batchRows: BatchRows[R, JdbcAction],
        mkRow: Project[E, R]): javadsl.Control = {
      val control = Projection.projectRows(
        batchSource.toScala,
        p,
        { rows: Seq[R] => batchRows(rows.asJava) })(e => mkRow(e).asScala)
      new javadsl.GrpcControl(control)
    }

    override def flow = {
      val con = createCon()
      val promise = Promise[Done]()
      val f = promise.future
      Flow[JdbcAction].map { a =>
        a.execute(con)
      }.alsoTo(Sink.onComplete {
        case _ =>
          Try(con.close())
          val _ = promise.trySuccess(Done)
      })
        .withAttributes(SProjector.blockingDispatcherAttrs)
        .mapMaterializedValue(_ =>
          new ProjectorResource() {
            def cancel() = {
              Try(con.close())
              val _ = promise.trySuccess(Done)
              f
            }
            def closed = f
          })
    }
  }
}
