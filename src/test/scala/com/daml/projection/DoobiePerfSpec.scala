// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.NotUsed
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.testkit.TestKit
import cats.effect.unsafe.implicits.global
import com.daml.ledger.api.v1.event._
import com.daml.ledger.api.v1.value.{ List => _, Map => _, _ }
import doobie._
import doobie.implicits._
import org.scalatest._
import org.scalatest.wordspec._
import org.scalatest.matchers.must._
import com.daml.projection.scaladsl.Doobie

// TODO(daml/15692) improvements https://github.com/digital-asset/daml/issues/15692
class DoobiePerfSpec
    extends TestKit(ActorSystem("PerfSpec"))
    with AnyWordSpecLike
    with GivenWhenThen
    with Matchers
    with EitherValues
    with BeforeAndAfterAll
    with TestEmbeddedPostgres {
  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  "A projection" must {
    "be able to ingest X events/second" in {
      implicit val projector = scaladsl.Doobie.Projector()

      val projection: Projection[ExercisedEvent] = mkChoice
      projector.getOffset(projection) must be(None)
      println("Preparing events")
      val nrTxs = 200
      val nrEventsPerTx = 10000
      val generatedSize = nrTxs * nrEventsPerTx

      def mkEvent(ix: Int, txId: String, projection: Projection[ExercisedEvent], offset: Offset) =
        Envelope(
          ExercisedEvent(
            eventId = "event-id-" + ix,
            contractId = "contract-id",
            templateId = Some(templateId),
            choice = choice,
            choiceArgument = None,
            actingParties = Seq(partyIdBob),
            consuming = false,
            witnessParties = Seq(partyIdBob),
            exerciseResult = None
          ),
          None,
          None,
          Some(txId),
          Some(offset),
          projection.table
        )

      def mkTx(tx: Int): Seq[ConsumerRecord[ExercisedEvent]] = {
        val txId = f"tx-$tx%07d"
        val offset = Offset(f"offset-$tx%07d")
        (1 to nrEventsPerTx).map { ePerTx =>
          mkEvent(ePerTx, txId, projection, offset)
        } ++ List(TxBoundary(projection.id, offset))
      }

      val source: Source[ConsumerRecord[ExercisedEvent], NotUsed] =
        Source
          .unfold(0) {
            case txNr if txNr >= nrTxs => None
            case txNr                  => Some((txNr + 1) -> mkTx(txNr + 1))
          }
          .mapConcat(identity)

      runIO(doobie.postgres.hi.connection.pgSetPrepareThreshold(3))

      println("Prepared events")

      val start = System.currentTimeMillis

      // for updateMany, bulk insert / update
      case class ExercisedEventRow(
          contractId: String,
          eventId: String,
          actingParties: String,
          witnessParties: String,
          offset: Option[Offset]
      )
      val projectionResults = source
        .via(Batcher(nrEventsPerTx, 10.seconds))
        .via(
          Projection.batchOperationFlow(
            scaladsl.Doobie.UpdateMany[ExercisedEventRow](
              s"insert into ${projection.table.name}(contract_id, event_id, acting_parties, witness_parties, event_offset) VALUES (?, ?, ?, ?, ?)"
            ),
            e => {
              val actingParties = e.event.actingParties.mkString(",")
              val witnessParties = e.event.witnessParties.mkString(",")
              List(ExercisedEventRow(
                e.event.contractId,
                actingParties,
                witnessParties,
                e.event.eventId,
                e.offset
              ))
            }
          ))
        .prepend(Source.single(Doobie.InitProjection(projection)))
        .via(projector.flow)
        .toMat(Sink.fold(0)(_ + _))(Keep.right)

      Then("the expected events should be projected")
      val result = projectionResults.run()
      // the total inserts for the events + update + a commit per tx of the projection
      // adding one for the insert into the projection table
      Await.result(result, 5000.seconds) must be(generatedSize + nrTxs + 1)
      val now = System.currentTimeMillis
      println(s"""
         |Elapsed time ${now - start}ms, ${generatedSize.toDouble / ((now - start).toDouble / 1000)} ops/sec.
         |Ingested $nrTxs transactions of $nrEventsPerTx events.
         |Total of ${generatedSize} records.""".stripMargin)
      val count =
        runIO(sql"select count(*) from exercised_events".query[String].unique)

      projector.getOffset(projection) must be(Some(Offset(f"offset-$nrTxs%07d")))
      Then("the projected table should contain the events")
      count.toInt must be(generatedSize)
      val tableSize =
        runIO(
          sql"SELECT pg_size_pretty( pg_total_relation_size('exercised_events') )"
            .query[String]
            .unique
        )
      // this assumes that this perf test generates a considerable amount of data.
      val size = tableSize.split(" ")(0)
      val MB = tableSize.split(" ")(1)
      MB must be("MB")
      println(s"${projection.table} size= $tableSize")
      println(s"size of an event =~ ${size.toDouble * 1024 / generatedSize}k")
    }
  }

  def runIO[R](conIO: ConnectionIO[R]) = conIO.transact(currentDb.xa).unsafeRunSync()

  val templateId = Identifier(
    packageId = "32b22221ccc8eb8ff6d6c5f62e25caec901196798cb52059e4596e7ce42b7e33",
    moduleName = "Iou",
    entityName = "IouTransfer"
  )
  val choice = "IouTransfer_Accept"
  val partyIdBob = "Bob::1220ca5c20e3d9e47ade773119246e250f5384765d5302d3b37d21de7e0c07fd3d55"

  def mkChoice = {
    val projectionId = ProjectionId("id-1")

    val projectionTable = ProjectionTable("exercised_events")

    Projection[ExercisedEvent](
      projectionId,
      ProjectionFilter.parties(Set(partyIdBob)),
      projectionTable
    )
  }
}
