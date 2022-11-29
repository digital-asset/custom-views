// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.javadsl

import akka.Done

import com.daml.projection.{ scaladsl => S }
import java.util.concurrent.CompletionStage
import scala.jdk.FutureConverters._

trait Control {

  /**
   * Cancels the projection.
   */
  def cancel(): CompletionStage[Done]

  /**
   * Returns a `CompletionStage` that completes with the `Throwable` that caused the projection to fail.
   *
   * If the `Projection` is successful, and has completed, the returned `CompletionStage` is failed with a
   * `NoSuchElementException`. The returned `CompletionStage` will not complete if the `Projection` runs continuously
   * and does not fail.
   */
  def failed(): CompletionStage[Throwable]

  /**
   * Returns a `CompletionStage` that completes with `Done` when the projection has completed.
   *
   * The returned `CompletionStage` will not complete if the `Projection` runs continuously and does not fail.
   */
  def completed(): CompletionStage[Done]

  /**
   * Returns a `CompletionStage` that completes when all resources used by the projection have been closed.
   */
  def resourcesClosed(): CompletionStage[Done]
  def asScala: S.Control
}

private[projection] final class GrpcControl(control: S.Control) extends Control {
  override def failed(): CompletionStage[Throwable] = control.failed.asJava
  override def completed(): CompletionStage[Done] = control.completed.asJava
  override def cancel(): CompletionStage[Done] = control.cancel().asJava
  override def resourcesClosed(): CompletionStage[Done] = control.resourcesClosed.asJava
  override def asScala: S.Control = control
}
