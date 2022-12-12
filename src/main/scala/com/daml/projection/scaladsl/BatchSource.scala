// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.scaladsl

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import com.daml.ledger.api.v1.event.{ ArchivedEvent, CreatedEvent, Event, ExercisedEvent }
import com.daml.ledger.api.v1.event.Event.Event.{ Archived, Created, Empty }
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.{ event => SE }
import com.daml.ledger.api.v1.{ transaction => ST }
import com.daml.ledger.api.v1.{ transaction_filter => SF }
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.v1.value.Identifier.toJavaProto
import com.daml.ledger.javaapi.{ data => J }
import com.daml.projection.{ javadsl, Batch, Batcher, ConsumerRecord, Envelope, Projection }
import com.daml.projection.javadsl.BatchSource.{ GetContractTypeId => JGetContractTypeId, GetParties => JGetParties }

import scala.concurrent.{ Future, Promise }
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

/**
 * A Source of [[Batch]]s.
 */
trait BatchSource[E] {
  def src(projection: Projection[E])(implicit sys: ActorSystem): Source[Batch[E], Control]
  def toJava = new com.daml.projection.javadsl.BatchSource[E] {
    override def src(projection: Projection[E])(implicit
        sys: ActorSystem): akka.stream.javadsl.Source[Batch[E], com.daml.projection.javadsl.Control] =
      BatchSource.this.src(projection).asJava.mapMaterializedValue(_.asJava)
  }
}

object BatchSource {

  // TODO add a create method to create a source from protobuf files https://github.com/digital-asset/daml/issues/15659
  def apply[E: GetContractTypeId: GetParties](batches: Seq[Batch[E]]): BatchSource[E] =
    new BatchSource[E] {
      def src(projection: Projection[E])(implicit sys: ActorSystem): Source[Batch[E], Control] = {
        val control = new TestControl
        Source(batches)
          .map(filterBatch(projection))
          .viaMat(handleCompletion(control))(Keep.right)
      }
    }

  def apply[E: GetContractTypeId: GetParties](source: Source[Batch[E], NotUsed]): BatchSource[E] =
    new BatchSource[E] {
      def src(projection: Projection[E])(implicit sys: ActorSystem): Source[Batch[E], Control] = {
        val control = new TestControl
        source
          .map(filterBatch(projection))
          .viaMat(handleCompletion(control))(Keep.right)
      }
    }

  def fromRecords[E: GetContractTypeId: GetParties](records: Seq[ConsumerRecord[E]]): BatchSource[E] =
    new BatchSource[E] {
      def src(projection: Projection[E])(implicit sys: ActorSystem): Source[Batch[E], Control] = {
        val control = new TestControl
        Source(records)
          .via(Batcher(Consumer.getBatchSize(projection), Consumer.getDefaultBatcherInterval()))
          .map(filterBatch(projection))
          .viaMat(handleCompletion(control))(Keep.right)
      }
    }

  private def filterEvent[E](transactionFilter: TransactionFilter)(event: E)(implicit
      getContractTypeId: GetContractTypeId[E],
      getParties: GetParties[E]) = {
    val templateIds = getTemplateIdsFromFilter(transactionFilter)
    val parties = getPartiesFromFilter(transactionFilter)
    getContractTypeId.from(event).forall(templateIds.contains(_)) &&
    getParties.from(event).forall(parties.contains(_))
  }

  // TODO https://github.com/digital-asset/daml/issues/15658 filter using transactionFilter
  private def filterEnvelope[E: GetContractTypeId: GetParties](projection: Projection[E])(e: Envelope[E]) =
    projection.predicate(e) &&
      projection.offset.forall(o => e.offset.forall(_.value >= o.value)) &&
      projection.endOffset.forall(o => e.offset.forall(_.value <= o.value)) &&
      filterEvent(projection.transactionFilter)(e.event)

  private def filterBatch[E: GetContractTypeId: GetParties](projection: Projection[E])(b: Batch[E]): Batch[E] =
    b.copy(envelopes = b.envelopes.filter(filterEnvelope(projection)))

  private def handleCompletion[T](control: Control): Flow[T, T, Control] =
    Flow[T].mapMaterializedValue(_ => control)
      .alsoTo(
        Flow[Any]
          .fold(())(Keep.left)
          .recover { case t: Throwable =>
            control.tryError(t)
            ()
          }
          .map { _ =>
            control.tryComplete()
            Done
          }
          .to(Sink.ignore)
      )

  private class TestControl extends Control {
    val promise = Promise[Done]()
    val f = promise.future
    val failurePromise = Promise[Throwable]()
    val ff = failurePromise.future
    override def cancel(): Future[Done] = {
      tryComplete()
      f
    }
    override def failed: Future[Throwable] = ff
    override def resourcesClosed: Future[Done] = f
    override def asJava: javadsl.Control = new javadsl.GrpcControl(this)
    override private[projection] def tryError(t: Throwable) = failurePromise.trySuccess(t)
    override private[projection] def tryComplete() = {
      promise.trySuccess(Done)
      failurePromise.tryFailure(new NoSuchElementException())
    }
  }

  /**
   * Creates a [[BatchSource]] from a function that transforms CreatedEvent`s into `C`s.
   */
  def apply[C](clientSettings: GrpcClientSettings)(f: CreatedEvent => C): BatchSource[C] = new BatchSource[C] {
    def src(projection: Projection[C])(implicit sys: ActorSystem): Source[Batch[C], Control] = {
      val p: Projection[Event] = projection.convert {
        case Event(Created(c)) => Some(f(c))
        case _                 => None
      }
      Consumer.eventSource(clientSettings, p)
        .map {
          _.collect { case Event(Created(createdEvent)) =>
            createdEvent
          }
        }
        .map(_.map(f))
    }
  }

  private def getTemplateIdsFromFilter(transactionFilter: SF.TransactionFilter) =
    transactionFilter.filtersByParty.flatMap { case (_, filters) =>
      filters.getInclusive.templateIds
    }.toSet

  private def getPartiesFromFilter(transactionFilter: SF.TransactionFilter) = transactionFilter.filtersByParty.keySet

  private def removeTemplateIdFilters(transactionFilter: SF.TransactionFilter) =
    SF.TransactionFilter(transactionFilter.filtersByParty.map {
      case (k, _) => k -> SF.Filters()
    })

  private def convertTemplateIdFilterToPredicate[E](
      projection: Projection[E],
      templateIdFilter: (Set[Identifier], Envelope[E]) => Boolean): Projection[E] = {
    val templateIds = getTemplateIdsFromFilter(projection.transactionFilter)

    def updatePredicate = {
      projection
        .withTransactionFilter(removeTemplateIdFilters(projection.transactionFilter))
        .withPredicate(e => projection.predicate(e) && templateIdFilter(templateIds, e))
    }

    if (templateIds.nonEmpty) updatePredicate else projection
  }

  /** Creates a [[BatchSource]] of `Event` events. */
  def events(clientSettings: GrpcClientSettings) = new BatchSource[Event] {
    def src(
        projection: Projection[Event])(implicit sys: ActorSystem): Source[Batch[Event], Control] =
      Consumer.eventSource(clientSettings, projection)
  }

  /** Creates a [[BatchSource]] of `ExercisedEvent` events. */
  def exercisedEvents(clientSettings: GrpcClientSettings) = new BatchSource[SE.ExercisedEvent] {
    def src(projection: Projection[SE.ExercisedEvent])(implicit
        sys: ActorSystem): Source[Batch[SE.ExercisedEvent], Control] = {

      def templateIdFilter(templateIds: Set[Identifier], e: Envelope[SE.ExercisedEvent]): Boolean =
        e.event.templateId.forall(templateIds.contains(_))
      val updatedProjection = convertTemplateIdFilterToPredicate(projection, templateIdFilter)

      Consumer.exercisedEventSource(clientSettings, updatedProjection)
    }
  }

  /** Creates a [[BatchSource]] of `TreeEvent` events. */
  def treeEvents(clientSettings: GrpcClientSettings) = new BatchSource[ST.TreeEvent] {
    def src(projection: Projection[ST.TreeEvent])(implicit sys: ActorSystem): Source[Batch[ST.TreeEvent], Control] = {

      def templateIdFilter(templateIds: Set[Identifier], e: Envelope[ST.TreeEvent]): Boolean = {
        val templateId = e.event.kind match {
          case ST.TreeEvent.Kind.Created(createdEvent)     => createdEvent.templateId
          case ST.TreeEvent.Kind.Exercised(exercisedEvent) => exercisedEvent.templateId
          case _                                           => None
        }
        templateId.forall(templateIds.contains(_))
      }

      val updatedProjection = convertTemplateIdFilterToPredicate(projection, templateIdFilter)
      Consumer.treeEventSource(clientSettings, updatedProjection)
    }
  }

  /**
   * Returns an optional [[Identifier]] from `E`.
   *
   * It should be only used by [[BatchSource]] create methods which creates [[BatchSource]] for testing purposes. It can
   * be instantiated with `from` methods in object [[GetContractTypeId]].
   */
  trait GetContractTypeId[E] {

    /** Extracts an optional [[Identifier]] from `E`. */
    def from(event: E): Option[Identifier]

    /** Converts to java DSL types. */
    def toJava: JGetContractTypeId[E] = (event: E) =>
      from(event).map(i => J.Identifier.fromProto(toJavaProto(i))).toJava
  }

  /** Methods that instantiates [[GetContractTypeId]] from common event types */
  object GetContractTypeId {
    implicit val `from event`: GetContractTypeId[Event] = fromEvent
    def fromEvent: GetContractTypeId[Event] = {
      case Event(Created(createdEvent))   => Some(createdEvent.getTemplateId)
      case Event(Archived(archivedEvent)) => Some(archivedEvent.getTemplateId)
      case Event(Empty)                   => None
    }

    implicit val `from created event`: GetContractTypeId[CreatedEvent] = fromCreatedEvent
    def fromCreatedEvent: GetContractTypeId[CreatedEvent] = (createdEvent: CreatedEvent) => createdEvent.templateId

    implicit val `from archived event`: GetContractTypeId[ArchivedEvent] = fromArchivedEvent
    def fromArchivedEvent: GetContractTypeId[ArchivedEvent] = (archivedEvent: ArchivedEvent) => archivedEvent.templateId

    implicit val `from exercised event`: GetContractTypeId[ExercisedEvent] = fromExercisedEvent
    def fromExercisedEvent: GetContractTypeId[ExercisedEvent] =
      (exercisedEvent: ExercisedEvent) => exercisedEvent.templateId
  }

  /**
   * Returns an parties from `E`.
   *
   * It should be only used by [[BatchSource]] create methods which creates [[BatchSource]] for testing purposes. It can
   * be instantiated with `from` methods in object [[GetContractTypeId]]
   */
  trait GetParties[E] {

    /** Extracts an set of parties from `E`. */
    def from(event: E): Set[String]

    /** Converts to java DSL types. */
    def toJava: JGetParties[E] = (event: E) =>
      from(event).asJava
  }

  /** Methods that instantiates [[GetParties]] from common event types */
  object GetParties {
    implicit val `from event`: GetParties[Event] = fromEvent
    def fromEvent: GetParties[Event] = {
      case Event(Created(createdEvent))   => createdEvent.witnessParties.toSet
      case Event(Archived(archivedEvent)) => archivedEvent.witnessParties.toSet
      case Event(Empty)                   => Set.empty
    }

    implicit val `from created event`: GetParties[CreatedEvent] = fromCreatedEvent
    def fromCreatedEvent: GetParties[CreatedEvent] = (createdEvent: CreatedEvent) => createdEvent.witnessParties.toSet

    implicit val `from archived event`: GetParties[ArchivedEvent] = fromArchivedEvent
    def fromArchivedEvent: GetParties[ArchivedEvent] =
      (archivedEvent: ArchivedEvent) => archivedEvent.witnessParties.toSet

    implicit val `from exercised event`: GetParties[ExercisedEvent] = fromExercisedEvent
    def fromExercisedEvent: GetParties[ExercisedEvent] =
      (exercisedEvent: ExercisedEvent) => exercisedEvent.witnessParties.toSet
  }
}
