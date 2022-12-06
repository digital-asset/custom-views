// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection
package javadsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.daml.ledger.api.v1.event.ExercisedEvent
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.javaapi.data.{ Identifier => JIdentifier }
import com.daml.projection.scaladsl.Projector
import org.scalatest._
import org.scalatest.wordspec._
import org.scalatest.matchers.must._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.FunctionConverters._
import scala.jdk.FutureConverters._
import scala.compat.java8.OptionConverters

import java.util.{ Set => JSet }

class JdbcPerfSpec
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
  implicit val ec = system.dispatchers.lookup(Projector.BlockingDispatcherId)

  "A projection" must {
    "be able to ingest X events/second" in {
      val projector = JdbcProjector.create(ds, system)
      val nrTxs = 200
      val nrEventsPerTx = 10000
      val generatedSize = nrTxs * nrEventsPerTx
      val lastOffset = Offset(f"offset-$nrTxs%07d")
      val projection: Projection[ExercisedEvent] = mkChoice
        .withEndOffset(lastOffset)
      projector.getOffset(projection) must be(None)

      println("Preparing events")

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

      val mkRow: Project[ExercisedEvent, ExercisedEventRow] = { e =>
        val actingParties = e.event.actingParties.mkString(",")
        val witnessParties = e.event.witnessParties.mkString(",")
        List(ExercisedEventRow(
          contractId = e.event.contractId,
          eventId = e.event.eventId,
          actingParties = actingParties,
          witnessParties = witnessParties,
          offset = e.offset
        )).asJava
      }

      val templateIdFromEvent = { e: ExercisedEvent =>
        OptionConverters.toJava(e.templateId.map(i => JIdentifier.fromProto(Identifier.toJavaProto(i))))
      }.asJava
      val partySetFromEvent = { e: ExercisedEvent => JSet.copyOf(e.witnessParties.asJava) }.asJava

      val batchSource =
        BatchSource.create(source.via(Batcher(nrEventsPerTx, 1.second)).asJava, templateIdFromEvent, partySetFromEvent)
      val start = System.currentTimeMillis
      println("Starting projection.")

      val control = projector.projectRows(
        batchSource,
        projection,
        UpdateMany.create(
          Sql.binder[ExercisedEventRow](s"insert into ${projection.table.name}(contract_id, event_id, acting_parties, witness_parties, event_offset) VALUES (?, ?, ?, ?, ?)")
            .bind(1, _.contractId)
            .bind(2, _.eventId)
            .bind(3, _.actingParties)
            .bind(4, _.witnessParties)
            .bind(
              5,
              _.offset.map(_.value).getOrElse("")
            ) // TODO better Option(al) support https://github.com/digital-asset/daml/issues/15705
        ),
        mkRow
      )
      Await.ready(control.resourcesClosed().asScala, 5000.seconds)
      println("Projection completed.")

      import cats.effect.unsafe.implicits.global
      import com.daml.ledger.api.v1.value.{ List => _ }
      import doobie._
      import doobie.implicits._
      def runIO[R](conIO: ConnectionIO[R]) = conIO.transact(currentDb.xa).unsafeRunSync()

      val now = System.currentTimeMillis
      val opsSec = generatedSize.toDouble / ((now - start).toDouble / 1000)
      println(s"""
         |Elapsed time ${now - start}ms, ${opsSec} ops/sec.
         |Ingested $nrTxs transactions of $nrEventsPerTx events.
         |Total of ${generatedSize} records.""".stripMargin)
      val count =
        runIO(sql"select count(*) from exercised_events".query[String].unique)
      val maxOffset = runIO(sql"select max(event_offset) from exercised_events".query[Offset].unique)
      maxOffset must be(lastOffset)
      val offsets =
        runIO(sql"select distinct(event_offset) from exercised_events order by event_offset".query[Offset].to[List])
      offsets must contain theSameElementsAs ((1 to nrTxs).map(tx => Offset(f"offset-$tx%07d")))

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
      val eventSizeK = size.toDouble * 1024 / generatedSize
      println(s"size of an event =~ ${eventSizeK}k, expecting more than ${10000d / eventSizeK} ops/sec")
      opsSec must be.>(10000d / eventSizeK)
    }
  }

  val templateId = new Identifier(
    "32b22221ccc8eb8ff6d6c5f62e25caec901196798cb52059e4596e7ce42b7e33",
    "Iou",
    "IouTransfer"
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
  case class ExercisedEventRow(
      contractId: String,
      eventId: String,
      actingParties: String,
      witnessParties: String,
      offset: Option[Offset]
  )
}
