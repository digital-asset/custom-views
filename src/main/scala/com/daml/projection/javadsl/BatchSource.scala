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
import com.daml.ledger.javaapi.data.{ Identifier => JIdentifier }
import com.daml.projection.scaladsl.BatchSource.EventForFilter

import java.{ util => ju }
import scala.compat.java8.OptionConverters
import scala.jdk.CollectionConverters._

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

  private def eventForFilter[E](
      templateIdSetFromEvent: java.util.function.Function[E, ju.Optional[JIdentifier]],
      partySetFromEvent: java.util.function.Function[E, ju.Set[String]]) = new EventForFilter[E] {
    override def templateId(event: E): Option[Identifier] =
      OptionConverters.toScala(templateIdSetFromEvent.apply(event)).map(i => Identifier.fromJavaProto(i.toProto))
    override def partySet(event: E): Set[String] = partySetFromEvent.apply(event).asScala.toSet
  }

  /**
   * Creates a [[BatchSource]] from existing batches, useful for testing purposes.
   */
  def create[E](
      batches: java.lang.Iterable[Batch[E]],
      templateIdFromEvent: java.util.function.Function[E, ju.Optional[JIdentifier]],
      partySetFromEvent: java.util.function.Function[E, ju.Set[String]]): BatchSource[E] =
    S.BatchSource(batches.asScala.toList)(eventForFilter(templateIdFromEvent, partySetFromEvent)).toJava

  /**
   * Creates a [[BatchSource]] from existing batches from an akka.stream.javadsl.Source, useful for testing purposes.
   */
  def create[E](
      source: Source[Batch[E], NotUsed],
      templateIdSetFromEvent: java.util.function.Function[E, ju.Optional[JIdentifier]],
      partySetFromEvent: java.util.function.Function[E, ju.Set[String]]) =
    S.BatchSource(source.asScala)(eventForFilter(templateIdSetFromEvent, partySetFromEvent)).toJava

  /**
   * Creates a [[BatchSource]] from existing consumer records, useful for testing purposes.
   */
  def createFromRecords[E](
      records: java.lang.Iterable[ConsumerRecord[E]],
      templateIdSetFromEvent: java.util.function.Function[E, ju.Optional[JIdentifier]],
      partySetFromEvent: java.util.function.Function[E, ju.Set[String]]): BatchSource[E] =
    S.BatchSource.fromRecords(records.asScala.toList)(eventForFilter(templateIdSetFromEvent, partySetFromEvent)).toJava

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
}
