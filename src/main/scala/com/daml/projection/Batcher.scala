// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic }
import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import com.daml.projection
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._

/**
 * Batches [[ConsumerRecord]]s by `batchSize`. Keeps the last [[TxBoundary]] and its position in the batch. On upstream
 * finish, the accumulated records are batched together, irrespective of `batchSize`. A scheduler checks the current
 * batch periodically on the set `interval` and emits it if it is not empty and clears the internal current batch.
 */
private[projection] final case class Batcher[T](batchSize: Int, interval: FiniteDuration)
    extends GraphStage[FlowShape[ConsumerRecord[T], Batch[T]]]
    with StrictLogging {
  import scala.collection.mutable.ListBuffer
  require(batchSize > 0, "batchSize must be greater than 0")

  val in = Inlet[ConsumerRecord[T]]("Batcher.in")
  val out = Outlet[Batch[T]]("Batcher.out")
  override val shape = FlowShape.of(in, out)
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {
      private val buffer = ListBuffer.empty[Envelope[T]]
      private var batch: Option[Batch[T]] = None
      private var lastBoundary: Option[TxBoundary[T]] = None
      private var lastBoundaryIndex = 0

      override def preStart() = {
        scheduleWithFixedDelay("ProjectionBatcherTimer", interval, interval)
      }

      override protected def onTimer(timerKey: Any) = {
        if (buffer.nonEmpty) {
          createBatch()
          if (isAvailable(out)) emitBatch()
        }
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            emitBatch()
          }
        }
      )
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            elem match {
              case boundary: TxBoundary[T] =>
                logger.trace(s"Received $boundary")
                lastBoundaryIndex = buffer.size
                lastBoundary = Some(boundary)
              case envelope: Envelope[T] =>
                logger.trace(s"Received envelope: ${envelope} with offset: ${envelope.offset}, adding to buffer, currently containing ${buffer.size} elements")
                buffer += envelope
            }

            if (buffer.size >= batchSize) {
              createBatch()
            }
            emitBatch()
          }

          override def onUpstreamFinish(): Unit = {
            if (buffer.isEmpty && lastBoundary.isEmpty) completeStage()
            else {
              // There are elements left in buffer, so
              // we keep accepting downstream pulls and push from buffer until emptied.
              //
              // It might be though, that the upstream finished while it was pulled, in which
              // case we will not get an onPull from the downstream, because we already had one.
              // In that case we need to emit from the buffer.
              if (isAvailable(out)) {
                batch = batch.orElse(Some(projection.Batch(buffer.toVector, lastBoundary, lastBoundaryIndex)))
                emitBatch()
              }
            }
          }
        }
      )

      private def createBatch() = {
        logger.trace(
          s"Creating batch, buffer size: ${buffer.size}, boundary at ${lastBoundaryIndex}, boundary ${
              lastBoundary
                .map(_.offset)
            }"
        )
        batch = Some(Batch(buffer.toVector, lastBoundary, lastBoundaryIndex))
        lastBoundary = None
        lastBoundaryIndex = 0
        buffer.clear()
      }

      private def emitBatch(): Unit = {
        if (batch.isEmpty) {
          if (isClosed(in)) completeStage()
          else {
            if (!hasBeenPulled(in)) pull(in)
          }
        } else {
          batch.foreach { b =>
            logger.trace(s"Emitting batch $b")
            push(out, b)
            batch = None
            lastBoundary = None
            lastBoundaryIndex = 0
          }
        }
      }
    }
}
