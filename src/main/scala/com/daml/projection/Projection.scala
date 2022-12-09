// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.event.{ ArchivedEvent, CreatedEvent, Event }
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.javaapi.data.{ TransactionFilter => JTransactionFilter }
import com.daml.projection.Projection.Predicate
import scaladsl.Projector

import scala.jdk.FunctionConverters._
import scala.jdk.OptionConverters._

/**
 * A Projection is a resumable and persistent ledger query. The progress of a Projection is stored in the `projection`
 * table.
 */
trait Projection[E] {

  /** Returns the id of the projection */
  def id: ProjectionId

  /** Returns the offset where this projection is reading from the ledger. None means read from the beginning */
  def offset: Option[Offset]

  /** Returns the end offset where this projection is reading from the ledger. None means read continuously */
  def endOffset: Option[Offset]

  /** Returns the `TransactionFilter` that is derived from this projection */
  def transactionFilter: TransactionFilter

  /** Returns a copy of this Projection with the offset modified */
  def withOffset(offset: Offset): Projection[E]

  /** Returns a copy of this Projection with the end offset modified */
  def withEndOffset(endOffset: Offset): Projection[E]

  /**
   * Returns the batchSize for batching [[Batch]]es, if not set, the default in reference.conf is used
   */
  def batchSize: Option[Int]

  /** Returns a copy of this Projection with the batchSize modified */
  def withBatchSize(batchSize: Int): ProjectionImpl[E]

  /**
   * Converts the projected (event) type of this projection to another representation. For instance to create a
   * projection of contracts from a projection of `Event`, only matching `CreatedEvent`s and converting those events
   * into a codegenerated Contract type.
   */
  def convert[E2](f: E2 => Option[E]): Projection[E2]

  /**
   * The predicate that this projection uses to filter events read from the ledger
   */
  def predicate: Projection.Predicate[E]

  /**
   * Returns a copy of this Projection with the predicate modified
   */
  def withPredicate(predicate: Projection.Predicate[E]): Projection[E]

  /**
   * Java API
   *
   * Returns a copy of this Projection with the predicate modified
   */
  def withPredicate(jpredicate: java.util.function.Predicate[Envelope[E]]): Projection[E] =
    withPredicate(jpredicate.asScala)

  /**
   * Java API
   *
   * Returns the offset where this projection is reading from the ledger. None means read from the beginning
   */
  def getOffset(): java.util.Optional[Offset] = offset.toJava

  /**
   * Java API
   *
   * Returns the `TransactionFilter` that is derived from this projection
   */
  def getTransactionFilter(): JTransactionFilter =
    JTransactionFilter.fromProto(TransactionFilter.toJavaProto(transactionFilter))

  private[projection] def withTransactionFilter(transactionFilter: TransactionFilter): Projection[E]
}

/**
 * [[Projection]] companion, creates [[Projection]]s, provides methods for projecting Ledger events from a
 * [[scaladsl.BatchSource]] into some storage, using a [[scaladsl.Projector]] which executes actions.
 */
object Projection extends {

  type Predicate[E] = Envelope[E] => Boolean

  type Project[E, A] = Envelope[E] => Iterable[A]

  type Advance[A] = (ProjectionId, Offset) => A

  type Init[A] = Projection[_] => A

  /**
   * Java API
   *
   * Creates a [[Projection]] of ledger events of type `E` filtered by `filter`. If the projection already exists in the
   * projection table, it will continue from the stored offset.
   */
  def create[E](
      id: ProjectionId,
      filter: ProjectionFilter
  ): Projection[E] = Projection[E](
    id,
    filter
  )

  /**
   * Java API
   *
   * Creates a [[Projection]] of ledger events of type `E` filtered by `filter`, reading from `offset` specified.
   */
  def create[E](
      id: ProjectionId,
      filter: ProjectionFilter,
      offset: Offset
  ): Projection[E] = Projection[E](
    id,
    filter,
    Some(offset)
  )

  /**
   * Creates a [[Projection]] of ledger events of type `E` filtered by `filter`, reading from `offset` specified.
   */
  def apply[E](
      id: ProjectionId,
      filter: ProjectionFilter,
      offset: Option[Offset] = None
  ): Projection[E] =
    ProjectionImpl[E](id, filter.transactionFilter, offset)

  /**
   * Projects the projection using a [[Project]] function that creates actions from the event. Events of type `E` are
   * rescaladsl.BatchSource]]. Actions are executed by the supplied [[scaladsl.Projector]].
   */
  def project[E, A: Projector](
      batchSource: scaladsl.BatchSource[E],
      projection: Projection[E]
  )(f: Project[E, A])(implicit as: akka.actor.ActorSystem): scaladsl.Control = {
    val projector = implicitly[Projector[A]]
    val currentOffset = projector.getOffset(projection)
    val updatedProjection = currentOffset.map(o => projection.withOffset(o)).getOrElse(projection)
    run(batchSource.src(updatedProjection)
      .via(Flows.project(projection, f)))
  }

  /**
   * Projects the projection, using a [[Project]] function that creates `R` rows, using a `batchRows` function that
   * batches a list of rows into one action. Events of type `E` are read from a [[scaladsl.BatchSource]]. Actions are
   * executed by the supplied [[scaladsl.Projector]].
   */
  def projectRows[E, A: Projector, R](
      batchSource: scaladsl.BatchSource[E],
      projection: Projection[E],
      batchRows: Seq[R] => A)(
      projectRow: Project[E, R])(implicit sys: ActorSystem): scaladsl.Control = {
    val projector = implicitly[Projector[A]]
    val currentOffset = projector.getOffset(projection)
    val updatedProjection = currentOffset.map(o => projection.withOffset(o)).getOrElse(projection)
    val initAction = projector.init(projection)
    run(batchSource.src(updatedProjection)
      .via(batchOperationFlow(batchRows, projectRow).prepend(Source.single(initAction))))
  }

  private[projection] def batchOperationFlow[E, R, A](f: Seq[R] => A, project: Project[E, R])(implicit
      projector: Projector[A]) =
    Flow[Batch[E]].mapConcat { batch =>
      val (before, after) = batch.split
      val beforeRows = before.flatMap(project)
      val afterRows = after.flatMap(project)
      val boundaryAction = batch.boundary.map(b => projector.advance(b.projectionId, b.offset))
      f(beforeRows) +: boundaryAction.toSeq :+ f(afterRows)
    }

  /**
   * Projects the `Event` projection, using a `fc` [[Project]] function that creates actions from `CreatedEvent`s and a
   * `fa` [[Project]] function that creates actions from `ArchivedEvent`s. Actions are executed by the supplied
   * [[scaladsl.Projector]].
   */
  def project[A: Projector](
      batchSource: scaladsl.BatchSource[Event],
      projection: Projection[Event],
      fc: Project[CreatedEvent, A],
      fa: Project[ArchivedEvent, A])(implicit sys: ActorSystem): scaladsl.Control =
    project(batchSource, projection)(Projection.fromCreatedOrArchived(fc, fa))

  private def run[A: Projector](source: Source[A, scaladsl.Control])(implicit sys: ActorSystem): scaladsl.Control = {
    source.viaMat(implicitly[Projector[A]].flow) { (sourceControl, projectorResource) =>
      implicit val dispatcher = sys.dispatchers.lookup(Projector.BlockingDispatcherId)
      new scaladsl.Control() {
        override def asJava =
          throw new UnsupportedOperationException("Cannot convert Control to javadsl after materialization.")
        override def cancel() = sourceControl.cancel().flatMap(_ => projectorResource.cancel())
        override def failed = sourceControl.failed
        override def tryError(t: Throwable): Boolean = sourceControl.tryError(t)
        override def tryComplete(): Boolean = sourceControl.tryComplete()
        override def resourcesClosed = sourceControl.resourcesClosed.flatMap(_ => projectorResource.closed)
      }
    }
      .toMat(Sink.ignore)(Keep.left)
      .run()
  }

  /** creates an `Event` Project function from a `CreatedEvent` [[Project]] function */
  def fromCreated[A](f: Project[CreatedEvent, A]): Project[Event, A] = {
    val mapped: (Envelope[Event] => Option[Envelope[CreatedEvent]]) = e =>
      e.traverseOption {
        case Event(Created(createdEvent)) => createdEvent
      }
    e => mapped(e).toList.flatMap(f)
  }

  /**
   * creates an `Event` Project function from a `CreatedEvent` [[Project]] function and an `ArchivedEvent` [[Project]]
   * function
   */
  def fromCreatedOrArchived[A](
      fc: Project[CreatedEvent, A],
      fa: Project[ArchivedEvent, A]): Project[Event, A] = envelope => {
    val res = envelope.event.event match {
      case Event.Event.Created(e)  => fc(envelope.withEvent(e))
      case Event.Event.Archived(e) => fa(envelope.withEvent(e))
      case _                       => List()
    }
    res
  }

  private[projection] object Flows {

    def project[E, A](
        projection: Projection[E],
        f: Project[E, A]
    )(implicit projector: Projector[A]): Flow[Batch[E], A, NotUsed] = {
      Flow[Batch[E]].mapConcat { b =>
        b.project(f, (cId, offset) => projector.advance(cId, offset))
      }.prepend(Source.single(implicitly[Projector[A]].init(projection)))
    }

    def project[A](
        projection: Projection[Event],
        fc: Project[CreatedEvent, A],
        fa: Project[ArchivedEvent, A])(implicit projector: Projector[A]): Flow[Batch[Event], A, NotUsed] = {
      val f: Project[Event, A] = Function.unlift { envelope =>
        envelope.event.event match {
          case Event.Event.Created(e)  => Some(fc(envelope.withEvent(e)))
          case Event.Event.Archived(e) => Some(fa(envelope.withEvent(e)))
          case _                       => None
        }
      }
      project(projection, e => f(e))
    }
  }
}

private[projection] final case class ProjectionImpl[E](
    id: ProjectionId,
    transactionFilter: TransactionFilter,
    offset: Option[Offset],
    predicate: Projection.Predicate[E] = (_: Envelope[E]) => true,
    endOffset: Option[Offset] = None,
    batchSize: Option[Int] = None
) extends Projection[E] {
  override def withOffset(offset: Offset): ProjectionImpl[E] = copy(offset = Some(offset))
  override def withEndOffset(endOffset: Offset): ProjectionImpl[E] = copy(endOffset = Some(endOffset))
  override def withBatchSize(batchSize: Int): ProjectionImpl[E] = copy(batchSize = Some(batchSize))
  override def withPredicate(predicate: Predicate[E]): ProjectionImpl[E] = copy(predicate = predicate)

  def convert[E2](f: E2 => Option[E]): Projection[E2] =
    ProjectionImpl[E2](
      id,
      transactionFilter,
      offset,
      env => env.traverseOption(f.unlift).map(predicate).getOrElse(false),
      endOffset,
      batchSize)

  private[projection] def withTransactionFilter(transactionFilter: TransactionFilter): Projection[E] = {
    copy(transactionFilter = transactionFilter)
  }
}
