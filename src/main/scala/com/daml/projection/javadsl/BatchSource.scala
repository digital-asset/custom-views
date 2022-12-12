// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.javadsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.javadsl.Source
import akka.stream.scaladsl.{ Source => SSource }
import com.daml.projection.{ scaladsl => S, Batch, ConsumerRecord, Projection }
import com.daml.ledger.javaapi.{ data => J }
import com.daml.ledger.api.v1.{ event => SE }
import com.daml.ledger.api.v1.event.Event.Event.{ Created => SCreated }
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.v1.{ transaction => ST }
import com.daml.projection.scaladsl.BatchSource.{ GetContractTypeId => SGetContractTypeId, GetParties => SGetParties }

import java.util.Optional
import java.{ util => ju }
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

/**
 * A Source of [[Batch]]s.
 */
trait BatchSource[E] {
  def src(projection: Projection[E])(implicit sys: ActorSystem): Source[Batch[E], Control]

  def toScala = new com.daml.projection.scaladsl.BatchSource[E] {
    override def src(projection: Projection[E])(implicit sys: ActorSystem): SSource[Batch[E], S.Control] =
      BatchSource.this.src(projection).asScala.mapMaterializedValue(_.asScala)
  }
}

object BatchSource {

  /**
   * Creates a [[BatchSource]] from existing batches, useful for testing purposes.
   */
  def create[E](
      batches: java.lang.Iterable[Batch[E]],
      contractTypeIdFromEvent: GetContractTypeId[E],
      partiesFromEvent: GetParties[E]): BatchSource[E] =
    S.BatchSource(batches.asScala.toList)(
      contractTypeIdFromEvent.toScala,
      partiesFromEvent.toScala).toJava

  /**
   * Creates a [[BatchSource]] from existing batches from an akka.stream.javadsl.Source, useful for testing purposes.
   */
  def create[E](
      source: Source[Batch[E], NotUsed],
      contractTypeIdFromEvent: GetContractTypeId[E],
      partiesFromEvent: GetParties[E]): BatchSource[E] =
    S.BatchSource(source.asScala)(
      contractTypeIdFromEvent.toScala,
      partiesFromEvent.toScala).toJava

  /**
   * Creates a [[BatchSource]] from existing consumer records, useful for testing purposes.
   */
  def createFromRecords[E](
      records: java.lang.Iterable[ConsumerRecord[E]],
      contractTypeIdFromEvent: GetContractTypeId[E],
      partiesFromEvent: GetParties[E]): BatchSource[E] =
    S.BatchSource.fromRecords(records.asScala.toList)(
      contractTypeIdFromEvent.toScala,
      partiesFromEvent.toScala).toJava

  /**
   * Creates a [[BatchSource]] from a function that transforms CreatedEvent`s into `Ct`s.
   */
  def create[Ct](
      clientSettings: GrpcClientSettings,
      f: java.util.function.Function[J.CreatedEvent, Ct]): BatchSource[Ct] = {
    import scala.jdk.FunctionConverters._
    val sf = f.asScala
    new BatchSource[Ct] {
      def src(projection: Projection[Ct])(implicit sys: ActorSystem): Source[Batch[Ct], Control] = {
        val p: Projection[SE.Event] = projection.convert {
          case SE.Event(SCreated(c)) => Some(sf(J.CreatedEvent.fromProto(SE.CreatedEvent.toJavaProto(c))))
          case _                     => None
        }
        S.Consumer.eventSource(clientSettings, p)
          .map {
            _.collect { case SE.Event(SCreated(createdEvent)) =>
              createdEvent
            }
          }
          .map { b =>
            b.map(ce => sf(J.CreatedEvent.fromProto(SE.CreatedEvent.toJavaProto(ce))))
          }
      }.asJava.mapMaterializedValue(c => c.asJava)
    }
  }

  /** Creates [[BatchSource]] of `Event` events. */
  def events(clientSettings: GrpcClientSettings) = new S.BatchSource[J.Event] {
    def src(
        projection: Projection[J.Event])(implicit sys: ActorSystem): SSource[Batch[J.Event], S.Control] = {

      S.BatchSource.events(clientSettings)
        .src(
          projection.convert(sp => Some(J.Event.fromProtoEvent(SE.Event.toJavaProto(sp)))))
        .map(_.map(event =>
          J.Event.fromProtoEvent(SE.Event.toJavaProto(event))))
    }
  }.toJava

  /** Creates a [[BatchSource]] of `ExercisedEvent` events. */
  def exercisedEvents(clientSettings: GrpcClientSettings) = new S.BatchSource[J.ExercisedEvent] {
    def src(
        projection: Projection[J.ExercisedEvent])(implicit
        sys: ActorSystem): SSource[Batch[J.ExercisedEvent], S.Control] = {
      S.BatchSource.exercisedEvents(clientSettings)
        .src(
          projection.convert(sp => Some(J.ExercisedEvent.fromProto(SE.ExercisedEvent.toJavaProto(sp))))
        )
        .map(_.map(event =>
          J.ExercisedEvent.fromProto(SE.ExercisedEvent.toJavaProto(event))))
    }
  }.toJava

  /** Creates a [[BatchSource]] of `TreeEvent` events. */
  def treeEvents(clientSettings: GrpcClientSettings) = new S.BatchSource[J.TreeEvent] {
    def src(
        projection: Projection[J.TreeEvent])(implicit sys: ActorSystem): SSource[Batch[J.TreeEvent], S.Control] = {
      S.BatchSource.treeEvents(clientSettings)
        .src(
          projection.convert(sp => Some(J.TreeEvent.fromProtoTreeEvent(ST.TreeEvent.toJavaProto(sp))))
        )
        .map(_.map(event =>
          J.TreeEvent.fromProtoTreeEvent(ST.TreeEvent.toJavaProto(event))))
    }
  }.toJava

  /**
   * Returns an optional `Identifier` from `E`.
   *
   * It should be only used by [[BatchSource]] create methods which creates [[BatchSource]] for testing purposes. It can
   * be instantiated with `from` methods in object [[GetContractTypeId]].
   */
  @FunctionalInterface
  trait GetContractTypeId[E] {

    /** Extracts an optional `Identifier` from `E`. */
    def from(event: E): ju.Optional[J.Identifier]

    /** Converts to scala DSL types. */
    def toScala: SGetContractTypeId[E] = (event: E) =>
      from(event).toScala.map(i => Identifier.fromJavaProto(i.toProto))
  }

  /** Provides methods that create [[GetContractTypeId]]s from common event types */
  object GetContractTypeId {
    implicit val `from event`: GetContractTypeId[J.Event] = fromEvent()
    def fromEvent(): GetContractTypeId[J.Event] = (event: J.Event) => {
      val se = SE.Event.fromJavaProto(event.toProtoEvent)
      SGetContractTypeId.fromEvent.toJava.from(se)
    }

    implicit val `from created event`: GetContractTypeId[J.CreatedEvent] = fromCreatedEvent()
    def fromCreatedEvent(): GetContractTypeId[J.CreatedEvent] =
      (createdEvent: J.CreatedEvent) => Optional.of(createdEvent.getTemplateId)

    implicit val `from archived event`: GetContractTypeId[J.ArchivedEvent] = fromArchivedEvent()
    def fromArchivedEvent(): GetContractTypeId[J.ArchivedEvent] =
      (archivedEvent: J.ArchivedEvent) => Optional.of(archivedEvent.getTemplateId)

    implicit val `from exercised event`: GetContractTypeId[J.ExercisedEvent] = fromExercisedEvent()
    def fromExercisedEvent(): GetContractTypeId[J.ExercisedEvent] =
      (exercisedEvent: J.ExercisedEvent) => Optional.of(exercisedEvent.getTemplateId)
  }

  /**
   * Returns an parties from `E`.
   *
   * It should be only used by [[BatchSource]] create methods which creates [[BatchSource]] for testing purposes. It can
   * be instantiated with `from` methods in object [[GetParties]]
   */
  @FunctionalInterface
  trait GetParties[E] {

    /** Extracts an set of parties from `E`. */
    def from(event: E): ju.Set[String]

    /** Converts to scala DSL types. */
    def toScala: SGetParties[E] = (event: E) => from(event).asScala.toSet
  }

  /** Methods that instantiates [[GetParties]] from common event types */
  object GetParties {
    implicit val `from event`: GetParties[J.Event] = fromEvent()
    def fromEvent(): GetParties[J.Event] = (event: J.Event) => {
      val se = SE.Event.fromJavaProto(event.toProtoEvent)
      SGetParties.fromEvent.toJava.from(se)
    }

    implicit val `from created event`: GetParties[J.CreatedEvent] = fromCreatedEvent()
    def fromCreatedEvent(): GetParties[J.CreatedEvent] =
      (createdEvent: J.CreatedEvent) => ju.Set.copyOf(createdEvent.getWitnessParties)

    implicit val `from archived event`: GetParties[J.ArchivedEvent] = fromArchivedEvent()
    def fromArchivedEvent(): GetParties[J.ArchivedEvent] =
      (archivedEvent: J.ArchivedEvent) => ju.Set.copyOf(archivedEvent.getWitnessParties)

    implicit val `from exercised event`: GetParties[J.ExercisedEvent] = fromExercisedEvent()
    def fromExercisedEvent(): GetParties[J.ExercisedEvent] =
      (exercisedEvent: J.ExercisedEvent) => ju.Set.copyOf(exercisedEvent.getWitnessParties)
  }
}
