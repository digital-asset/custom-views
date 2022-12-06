// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection
package javadsl

import java.util.Optional
import com.daml.ledger.javaapi.{ data => J }
import scala.jdk.OptionConverters._

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
      batchSource: javadsl.BatchSource[E],
      p: Projection[E],
      f: Project[E, A]): javadsl.Control

  /**
   * Projects the `Event` projection, using a `fc` function that creates actions from `CreatedEvent`s and a `fa`
   * function that creates actions from `ArchivedEvent`s.
   */
  def projectEvents(
      batchSource: javadsl.BatchSource[J.Event],
      p: Projection[J.Event],
      fc: Project[J.CreatedEvent, A],
      fa: Project[J.ArchivedEvent, A]): javadsl.Control

  /**
   * Projects the projection, using a function that creates `R` rows from every event read from the ledger, using a
   * [[BatchRows]] function that batches the list of rows into one action. Events of type `E` are read from a
   * [[javadsl.BatchSource]].
   */
  def projectRows[E, R](
      batchSource: javadsl.BatchSource[E],
      p: Projection[E],
      batchRows: BatchRows[R, A],
      mkRow: Project[E, R]): javadsl.Control

  /**
   * Gets the current stored offset for the projection provided.
   */
  def getCurrentOffset(projection: Projection[_]): Optional[Offset] = getOffset(projection).toJava
}
