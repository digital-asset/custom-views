// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import java.time.Instant
import scala.compat.java8.OptionConverters

/**
 * A record consumed from the ledger of type `T`: an event, or a type derived from events
 */
sealed trait ConsumerRecord[T]

/**
 * Creates [[TxBoundary]]s
 */
object TxBoundary {

  /**
   * Creates the [[TxBoundary]]
   */
  def create[T](projectionId: ProjectionId, offset: Offset): TxBoundary[T] =
    TxBoundary(projectionId, offset)
}

/**
 * A Transactional boundary detected while running a Projection identified by `projectionId`, at the `offset` [[Offset]]
 */
final case class TxBoundary[T](projectionId: ProjectionId, offset: Offset)
    extends ConsumerRecord[T] {
  def coerce[B]: TxBoundary[B] = copy()

  /**
   * Gets the [[ProjectionId]]
   */
  def getProjectionId() = projectionId

  /**
   * Gets the [[Offset]]
   */
  def getOffset(): Offset = offset
}

/**
 * Creates [[Envelope]]s
 */
object Envelope {

  /**
   * Creates an [[Envelope]]
   */
  def create[T](
      event: T,
      workflowId: java.util.Optional[String],
      ledgerEffectiveTime: java.util.Optional[Instant],
      transactionId: java.util.Optional[String],
      offset: java.util.Optional[Offset]
  ) = Envelope(
    event,
    OptionConverters.toScala(workflowId),
    OptionConverters.toScala(ledgerEffectiveTime),
    OptionConverters.toScala(transactionId),
    OptionConverters.toScala(offset)
  )
}

/**
 * Wraps a value of type `T` along with information about the value from the ledger.
 */
final case class Envelope[T](
    event: T,
    workflowId: Option[String],
    ledgerEffectiveTime: Option[Instant],
    transactionId: Option[String],
    offset: Option[Offset]
) extends ConsumerRecord[T] {

  /** Returns the wrapped value */
  def unwrap = event

  /** Returns an Envelope with the `event` modified */
  def withEvent[B](event: B): Envelope[B] = map(_ => event)

  /**
   * Maps the event, returning an envelope with an event as the result of `f`, keeping all other fields.
   */
  def map[B](f: T => B): Envelope[B] =
    Envelope[B](f(event), workflowId, ledgerEffectiveTime, transactionId, offset)

  /**
   * applies the partial function `f` to the wrapped event, returns some envelope with the result of the function (if
   * the partial function matches the event), otherwise returns none
   */
  def traverseOption[B](f: PartialFunction[T, B]): Option[Envelope[B]] =
    f.lift(event).map(e => this.copy(event = e))

  /**
   * Java API
   *
   * Gets the wrapped value in the envelope
   */
  def getEvent() = event

  /**
   * Java API
   *
   * Gets the ledgerEffectiveTime
   */
  def getLedgerEffectiveTime(): java.util.Optional[Instant] = OptionConverters.toJava(ledgerEffectiveTime)

  /**
   * Java API
   *
   * Gets the transactionId
   */
  def getTransactionId(): java.util.Optional[String] = OptionConverters.toJava(transactionId)

  /**
   * Java API
   *
   * Gets the [[Offset]]
   */
  def getOffset(): java.util.Optional[Offset] = OptionConverters.toJava(offset)
}
