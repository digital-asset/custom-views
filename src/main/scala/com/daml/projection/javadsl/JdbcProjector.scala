// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.javadsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Sink }
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.{ event => SE }
import com.daml.projection.{ scaladsl, Batch, Envelope, Migration, Offset, Projection, ProjectionId }
import com.daml.ledger.javaapi.{ data => J }
import com.daml.projection.Projection.Advance
import com.daml.projection.scaladsl.{ Projector => SProjector, ProjectorResource }
import com.typesafe.scalalogging.StrictLogging

import java.sql.SQLException
import java.util.Optional
import javax.sql.DataSource
import scala.concurrent.Promise
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
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
 * Projects ledger events into a destination by executing actions.
 * @tparam A
 *   the type of action
 */
trait Projector[A] extends scaladsl.Projector[A] {

  /**
   * Projects the projection using a function that creates actions from every event read from the Ledger. Events of type
   * `E` are read from a [[BatchSource]]. See [[BatchSource.events]], [[BatchSource.exercisedEvents]] and
   * [[BatchSource.treeEvents]] for default [[BatchSource]]s.
   */
  def project[E](
      batchSource: BatchSource[E],
      p: Projection[E],
      f: Project[E, A]): Control

  /**
   * Projects the `Event` projection, using a `fc` function that creates actions from `CreatedEvent`s and a `fa`
   * function that creates actions from `ArchivedEvent`s.
   */
  def projectEvents(
      batchSource: BatchSource[J.Event],
      p: Projection[J.Event],
      fc: Project[J.CreatedEvent, A],
      fa: Project[J.ArchivedEvent, A]): Control

  /**
   * Projects the projection, using a function that creates `R` rows from every event read from the ledger, using a
   * [[BatchRows]] function that batches the list of rows into one action. Events of type `E` are read from a
   * [[BatchSource]].
   */
  def projectRows[E, R](
      batchSource: BatchSource[E],
      p: Projection[E],
      batchRows: BatchRows[R, A],
      mkRow: Project[E, R]): Control

  /**
   * Gets the current stored offset for the projection provided.
   */
  def getCurrentOffset(projection: Projection[_]): Optional[Offset] = getOffset(projection).toJava
}

/**
 * Creates a [[Projector]] that executes [[JdbcAction]]s.
 */
object JdbcProjector {

  /**
   * Creates a JdbcProjector.
   */
  def create(
      ds: DataSource,
      system: ActorSystem): Projector[JdbcAction] = {
    new JdbcProjectorImpl(ds, system)
  }

  private final class JdbcProjectorImpl(
      ds: DataSource,
      system: ActorSystem)
      extends Projector[JdbcAction] with StrictLogging {
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
          |   projection_table,
          |   data,
          |   projection_type,
          |   projection_offset
          | )
          | values (
          |   ?,
          |   ?,
          |   ?::jsonb,
          |   ?,
          |   NULL
          | )
          """.stripMargin

        val insertIfNotExists = HandleError(
          ExecuteUpdate
            .create(sql)
            .bind(1, projection.id.value)
            .bind(2, projection.table.name)
            .bind(3, "{}")
            .bind(4, projection.getClass.getName),
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
        batchSource: BatchSource[E],
        p: Projection[E],
        f: Project[E, JdbcAction]): Control = {

      val control = Projection.project(batchSource.toScala, p)(e => f(e).asScala)
      new GrpcControl(control)
    }

    override def projectEvents(
        batchSource: BatchSource[J.Event],
        p: Projection[J.Event],
        fc: Project[J.CreatedEvent, JdbcAction],
        fa: Project[J.ArchivedEvent, JdbcAction]
    ): Control = {

      val sBatchSource = new BatchSource[SE.Event]() {
        override def src(projection: Projection[Event])(implicit
            sys: ActorSystem): akka.stream.javadsl.Source[Batch[Event], Control] = {
          batchSource.src(projection.convert(e => Some(SE.Event.fromJavaProto(e.toProtoEvent))))
        }.map(b => b.map(e => SE.Event.fromJavaProto(e.toProtoEvent)))
      }.toScala

      val control = Projection.project(
        sBatchSource,
        p.convert(e => Some(J.Event.fromProtoEvent(SE.Event.toJavaProto(e)))),
        c => fc(c.map(ce => J.CreatedEvent.fromProto(SE.CreatedEvent.toJavaProto(ce)))).asScala,
        a => fa(a.map(ae => J.ArchivedEvent.fromProto(SE.ArchivedEvent.toJavaProto(ae)))).asScala
      )
      new GrpcControl(control)
    }

    import scala.jdk.CollectionConverters._

    override def projectRows[E, R](
        batchSource: BatchSource[E],
        p: Projection[E],
        batchRows: BatchRows[R, JdbcAction],
        mkRow: Project[E, R]): Control = {
      val control = Projection.projectRows(
        batchSource.toScala,
        p,
        { rows: Seq[R] => batchRows(rows.asJava) })(e => mkRow(e).asScala)
      new GrpcControl(control)
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
