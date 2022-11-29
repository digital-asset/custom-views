// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import scala.compat.java8.OptionConverters
import scala.jdk.CollectionConverters._

/**
 * A Batch of [[Envelope]]s.
 */
object Batch {

  /**
   * Java API
   */
  def create[T](envelopes: java.lang.Iterable[Envelope[T]]): Batch[T] = Batch(envelopes.asScala.toVector)

  /**
   * Java API
   */
  def create[T](envelopes: java.lang.Iterable[Envelope[T]], boundary: TxBoundary[T]): Batch[T] =
    Batch(envelopes.asScala.toVector, boundary)

  def apply[T](envelopes: Vector[Envelope[T]]): Batch[T] = Batch(envelopes, None, 0)
  def apply[T](envelopes: Vector[Envelope[T]], boundary: TxBoundary[T]): Batch[T] =
    Batch(envelopes, Some(boundary), envelopes.size)
}

/**
 * A Batch of [[Envelope]]s, optionally has a transaction boundary that occurred on the Ledger.
 */
final case class Batch[E](
    envelopes: Vector[Envelope[E]],
    boundary: Option[TxBoundary[E]],
    boundaryIndex: Int
) {

  private[projection] def project[A](
      f: Projection.Project[E, A],
      advance: Projection.Advance[A]
  ): Vector[A] = {
    val (before, after) = envelopes.splitAt(boundaryIndex)
    before.flatMap(f) ++ boundary.map(b => advance(b.projectionId, b.offset)).toVector ++ after.flatMap(
      f
    )
  }

  private[projection] def split: (Vector[Envelope[E]], Vector[Envelope[E]]) = {
    envelopes.splitAt(boundaryIndex)
  }

  def map[B](f: E => B): Batch[B] = {
    copy(
      envelopes.map(_.map(f)),
      boundary.map(boundary => TxBoundary[B](boundary.projectionId, boundary.offset)),
      boundaryIndex
    )
  }

  def collect[B](f: PartialFunction[E, B]): Batch[B] = {
    copy(
      envelopes.flatMap(_.traverseOption(f)),
      boundary.map(_.coerce[B]),
      boundaryIndex
    )
  }

  def size = envelopes.size
  def nonEmpty = envelopes.nonEmpty
  def isEmpty = envelopes.isEmpty

  /**
   * Java API
   */
  def getEnvelopes(): java.util.List[Envelope[E]] = envelopes.asJava

  /**
   * Java API
   */
  def getBoundary(): java.util.Optional[TxBoundary[E]] = OptionConverters.toJava(boundary)

  /**
   * Java API
   */
  def getBoundaryIndex(): Int = boundaryIndex

  /**
   * Java API
   */
  def getSize() = envelopes.size

  /**
   * Java API
   */
  def getNonEmpty() = envelopes.nonEmpty

  /**
   * Java API
   */
  def getIsEmpty() = envelopes.isEmpty
}
