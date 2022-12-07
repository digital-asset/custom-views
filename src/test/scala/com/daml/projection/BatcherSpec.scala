// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Keep, Source }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest._
import org.scalatest.matchers.must._
import org.scalatest.wordspec._
import scala.concurrent.duration._

class BatcherSpec
    extends TestKit(ActorSystem("BatcherSpec"))
    with AnyWordSpecLike
    with Matchers
    with OptionValues {

  import TestUtil._
  val partyId = "alice"
  val interval = 10.seconds

  "A Batcher" must {
    "fail for batchSize < 1" in {
      an[IllegalArgumentException] must be thrownBy Batcher(0, interval)
    }

    "create n batches when batchSize * n is received" in {
      val projection = eventsProjection(partyId)
      def assertBatches(batchSize: Int, nrBatches: Int) = {
        val envelopes =
          (0 until batchSize * nrBatches)
            .map(i => Envelope(i, None, None, None, projection.offset))
            .toVector
        val probe =
          Source(envelopes).via(Batcher(batchSize, interval)).toMat(TestSink.probe)(Keep.right).run()
        val nrBatchesL = nrBatches.toLong
        val batches = probe.request(nrBatchesL).expectNextN(nrBatchesL)
        (0 until nrBatches).foldLeft(envelopes) { (nextEnvelopes, batchNr) =>
          val expectedBatch = Batch(nextEnvelopes.take(batchSize))
          val batch = batches(batchNr)
          batch must be(expectedBatch)
          batch.size must be(batchSize.toLong)
          nextEnvelopes.drop(batchSize)
        }
        probe.request(1).expectComplete()
      }
      for (batchSize <- 1 to 10) {
        for (nrBatches <- 1 to 10) {
          assertBatches(batchSize, nrBatches)
        }
      }
    }

    "create batches with tx boundaries" in {
      val projection = eventsProjection(partyId)
      val batchSize = 5
      val events = (0 until batchSize * 2).toVector
      val lastEvent = events.last
      // Every event is part of one transaction: Every one event is followed by one tx boundary.
      val envelopes: Vector[ConsumerRecord[Int]] =
        events.flatMap { i =>
          val offset = Offset(s"$i")
          List(
            Envelope(i, None, None, None, projection.offset),
            TxBoundary[Int](projection.id, offset)
          )
        }
      val probe = Source(envelopes).via(Batcher(batchSize, interval)).toMat(TestSink.probe)(Keep.right).run()

      val firstBatch = probe.request(1).expectNext()

      val lastEventInFirstBatchIx = batchSize - 1
      firstBatch.size must be(batchSize)
      firstBatch.envelopes.last.event must be(lastEventInFirstBatchIx)

      // Since every event is in its own tx, the tx of the last event fitting in the batch will not be part of the batch.
      val offsetAtBoundary = firstBatch.envelopes.last.event - 1
      firstBatch.boundary.value.offset.value must be(offsetAtBoundary.toString)
      val actions =
        firstBatch.project(e => List(e.event), (_, offset) => offset.value.toInt)
      val (committed, rest) = events.take(batchSize).splitAt(lastEventInFirstBatchIx)
      actions must be(committed ++ Vector(offsetAtBoundary) ++ rest)

      val secondBatch = probe.request(1).expectNext()
      secondBatch.size must be(batchSize)
      secondBatch.envelopes.last.event must be(batchSize * 2 - 1)
      val offsetAtBoundary2 = secondBatch.envelopes.last.event - 1
      secondBatch.boundary.value.offset must be(Offset(offsetAtBoundary2.toString))
      val actions2 =
        secondBatch.project(e => List(e.event), (_, offset) => offset.value.toInt)
      val (committed2, rest2) = events.drop(batchSize).splitAt(lastEventInFirstBatchIx)
      actions2 must be(committed2 ++ Vector(offsetAtBoundary2) ++ rest2)

      val lastBatchOnCompletion = probe.request(1).expectNext()
      lastBatchOnCompletion mustBe empty
      lastBatchOnCompletion.boundaryIndex must be(0)
      lastBatchOnCompletion.boundary.value.offset must be(Offset(lastEvent.toString))
      probe.request(1).expectComplete()
    }

    "create one batch that contains many transactions, keeping the last tx boundary found" in {
      val projection = eventsProjection(partyId)
      val batchSize = 10
      val events = (0 until batchSize).toVector
      val lastEvent = events.last
      // Every event is part of one transaction: Every one event is followed by one tx boundary.
      val envelopes: Vector[ConsumerRecord[Int]] =
        events.flatMap { i =>
          val offset = Offset(s"$i")
          List(
            Envelope(i, None, None, None, projection.offset),
            TxBoundary[Int](projection.id, offset)
          )
        }
      val probe = Source(envelopes).via(Batcher(batchSize, interval)).toMat(TestSink.probe)(Keep.right).run()

      val batch = probe.request(1).expectNext()

      val lastEventInBatchIx = batchSize - 1
      batch.size must be(batchSize)
      batch.envelopes.last.event must be(lastEventInBatchIx)

      // Since every event is in its own tx, the tx of the last event fitting in the batch will not be part of the batch.
      val offsetAtBoundary = batch.boundary.value.offset.value.toInt
      offsetAtBoundary must be(batch.envelopes.last.event - 1)
      offsetAtBoundary must be(events(lastEventInBatchIx - 1))
      batch.boundary.value.offset.value.toInt must be(offsetAtBoundary)
      // for easy assertions, action == offset
      val actions =
        batch.project(e => List(e.event), (_, offset) => offset.value.toInt)
      val (committed, rest) = events.take(batchSize).splitAt(lastEventInBatchIx)
      actions must be(committed ++ Vector(offsetAtBoundary) ++ rest)
      // the boundary adds 1
      actions.size must be(batchSize + 1)
      val lastBatchOnCompletion = probe.request(1).expectNext()
      lastBatchOnCompletion mustBe empty
      lastBatchOnCompletion.boundaryIndex must be(0)
      lastBatchOnCompletion.boundary.value.offset must be(Offset(lastEvent.toString))
      probe.request(1).expectComplete()
    }

    "create a batch containing < batchSize events, if less events are available on upstream finish" in {
      val projection = eventsProjection(partyId)

      val batchSize = 5
      val events = (0 until batchSize * 2 - 1).toVector
      // events are generated as index of event to easily check boundaries
      val envelopes: Vector[ConsumerRecord[Int]] =
        events.flatMap { i =>
          val offset = Offset(s"$i")
          List(
            Envelope(i, None, None, None, projection.offset),
            TxBoundary[Int](projection.id, offset)
          )
        }
      val probe = Source(envelopes).via(Batcher(batchSize, interval)).toMat(TestSink.probe)(Keep.right).run()

      val firstBatch = probe.request(1).expectNext()

      val lastEventInFirstBatchIx = batchSize - 1
      firstBatch.size must be(batchSize)
      firstBatch.envelopes.last.event must be(lastEventInFirstBatchIx)

      val offsetAtBoundary = firstBatch.envelopes.last.event - 1
      firstBatch.boundary.value.offset.value must be(offsetAtBoundary.toString)
      val actions =
        firstBatch.project(e => List(e.event), (_, offset) => offset.value.toInt)
      val (committed, rest) = events.take(batchSize).splitAt(lastEventInFirstBatchIx)
      actions must be(committed ++ Vector(offsetAtBoundary) ++ rest)

      val secondBatch = probe.request(1).expectNext()
      secondBatch.size must be(batchSize - 1)
      val offsetAtBoundary2 = secondBatch.envelopes.last.event
      secondBatch.boundary.value.offset must be(Offset(offsetAtBoundary2.toString))
      secondBatch.boundaryIndex must be(batchSize - 1)
      probe.request(1).expectComplete()
    }
  }
}
