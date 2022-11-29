// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.scaladsl

import akka.Done
import akka.stream.scaladsl.Flow
import akka.stream.{ ActorAttributes, Attributes }
import com.daml.projection.{ Offset, Projection }

import scala.concurrent.Future

/**
 * Projects ledger events into a destination by executing actions.
 * @tparam A
 *   action
 */
trait Projector[A] {

  /**
   * Returns the current offset for a projection
   * @param projection
   *   the projection
   * @return
   *   the offset or none if the projection has not started yet, or cannot be found
   */
  def getOffset(projection: Projection[_]): Option[Offset]

  /**
   * Executes actions, resulting in the number of rows affected per action.
   * @return
   *   a flow that executes actions
   */
  def flow: Flow[A, Int, ProjectorResource]

  /**
   * A function that creates a database action to update the offset of the projection.
   * @return
   *   the advance function
   */
  def advance: Projection.Advance[A]

  /**
   * A function that creates a database action to insert a projection row in the projection table if it does not already
   * exist.
   * @return
   *   the init function
   */
  def init: Projection.Init[A]
}

/**
 * Signals cancel to the Projector, allowing it to close its resources.
 */
trait ProjectorResource {
  def cancel(): Future[Done]
  def closed: Future[Done]
}

object Projector {
  val BlockingDispatcherId = "projection.blocking-io-dispatcher"
  val blockingDispatcherAttrs: Attributes =
    ActorAttributes.dispatcher(BlockingDispatcherId)
}
