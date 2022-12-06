// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.scaladsl

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import com.daml.ledger.api.v1.event.{ CreatedEvent, Event }
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.{ event => SE }
import com.daml.ledger.api.v1.{ transaction => ST }
import com.daml.ledger.api.v1.{ transaction_filter => SF }
import com.daml.ledger.api.v1.value.Identifier
import com.daml.projection.{ javadsl, Batch, Batcher, ConsumerRecord, Envelope, Projection }

import scala.concurrent.{ Future, Promise }

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
  trait EventForFilter[E] {
    def templateId(event: E): Option[Identifier]
    def partySet(event: E): Set[String]
  }

  // TODO add a create method to create a source from protobuf files https://github.com/digital-asset/daml/issues/15659
  def apply[E: EventForFilter](batches: Seq[Batch[E]]): BatchSource[E] =
    new BatchSource[E] {
      def src(projection: Projection[E])(implicit sys: ActorSystem): Source[Batch[E], Control] = {
        val control = new TestControl
        Source(batches)
          .map(filterBatch(projection))
          .viaMat(handleCompletion(control))(Keep.right)
      }
    }

  def apply[E: EventForFilter](source: Source[Batch[E], NotUsed]): BatchSource[E] =
    new BatchSource[E] {
      def src(projection: Projection[E])(implicit sys: ActorSystem): Source[Batch[E], Control] = {
        val control = new TestControl
        source
          .map(filterBatch(projection))
          .viaMat(handleCompletion(control))(Keep.right)
      }
    }

  def fromRecords[E: EventForFilter](records: Seq[ConsumerRecord[E]]): BatchSource[E] =
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
      eventForFilter: EventForFilter[E]) = {
    val templateIds = getTemplateIdsFromFilter(transactionFilter)
    val parties = getPartiesStrFromFilter(transactionFilter)
    eventForFilter.templateId(event).exists(templateIds.contains(_)) &&
    eventForFilter.partySet(event).exists(parties.contains(_))
  }

  // TODO https://github.com/digital-asset/daml/issues/15658 filter using transactionFilter
  private def filterEnvelope[E: EventForFilter](projection: Projection[E])(e: Envelope[E]) =
    projection.predicate(e) &&
      projection.offset.forall(o => e.offset.forall(_.value >= o.value)) &&
      projection.endOffset.forall(o => e.offset.forall(_.value <= o.value)) &&
      filterEvent(projection.transactionFilter)(e.event)

  private def filterBatch[E: EventForFilter](projection: Projection[E])(b: Batch[E]): Batch[E] =
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

  private def getPartiesStrFromFilter(transactionFilter: SF.TransactionFilter) = transactionFilter.filtersByParty.keySet

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
}
