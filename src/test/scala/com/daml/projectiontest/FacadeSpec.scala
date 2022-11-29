// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projectiontest

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.unsafe.implicits.global
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.event._
import com.daml.projection.{
  IouContract,
  Projection,
  ProjectionFilter,
  ProjectionId,
  ProjectionTable,
  SandboxHelper,
  TestEmbeddedPostgres
}
import com.daml.projection.scaladsl.{ BatchSource, Doobie }
import com.daml.quickstart.iou.iou._
import doobie._
import doobie.implicits._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must._
import org.scalatest.time._
import org.scalatest.wordspec._

@Ignore
class FacadeSpec
    extends TestKit(ActorSystem("FacadeSpec"))
    with AsyncWordSpecLike
    with GivenWhenThen
    with Matchers
    with BeforeAndAfterAll
    with EitherValues
    with ScalaFutures
    with OptionValues
    with TestEmbeddedPostgres
    with SandboxHelper {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(10, Millis)))

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  implicit val logHandler = LogHandler.nop
  val partyId = "alice"
  val projectionTable = ProjectionTable("ious")

  "A Projection" must {
    "provide an easy way to project events" in {
      import Doobie._
      import Projection._
      implicit val projector = Projector()
      val projectionId = ProjectionId("my-id")
      val events = Projection[Event](projectionId, ProjectionFilter.parties(Set(partyId)), projectionTable)

      val insert: Project[CreatedEvent, Action] = { envelope =>
        import envelope._
        val witnessParties = event.witnessParties.mkString(",")
        val iou = IouContract.toIou(event)

        List(fr"""insert into ${table.const}
          (contract_id, event_id, witness_parties, amount, currency)
          values (${event.contractId}, ${event.eventId}, $witnessParties, ${iou.data.amount}, ${iou.data.currency})""".update.run)
      }

      val delete: Project[ArchivedEvent, Action] = { envelope =>
        import envelope._
        List(fr"delete from ${table.const} where contract_id = ${event.contractId}".update.run)
      }
      val f = Projection.fromCreateOrArchive(insert, delete)
      implicit val source = BatchSource.events(clientSettings)
      val ctrl = Projection.project(source, events)(f)
      ctrl.cancel().map(_ mustBe Done)
    }

    "provide an easy way to project exercised events" in {
      import Doobie._

      implicit val projector = Doobie.Projector()
      val projectionId = ProjectionId("my-id")
      val exercisedEvents =
        Projection[ExercisedEvent](projectionId, ProjectionFilter.parties(Set(partyId)), ProjectionTable("contracts"))
      val source = BatchSource.exercisedEvents(clientSettings)

      val ctrl = Projection.project(source, exercisedEvents) {
        envelope =>
          import envelope._
          val actingParties = event.actingParties.mkString(",")
          val witnessParties = event.witnessParties.mkString(",")
          List(fr"""
          insert into ${table.const}
          (contract_id, event_id, acting_parties, witness_parties, event_offset)
          values (${event.contractId}, $actingParties, $witnessParties, ${event.eventId}, ${offset})
          """.update.run)
      }
      ctrl.cancel().map(_ mustBe Done)
    }

    "provide an easy way to project using a batch operation" in {
      implicit val projector = Doobie.Projector()
      val projectionId = ProjectionId("my-id")
      val events = Projection[Event](projectionId, ProjectionFilter.parties(Set(partyId)), projectionTable)
      case class CreatedRow(contractId: String, eventId: String, amount: BigDecimal, currency: String)
      val sql =
        s"""insert into ${events.table.name}(contract_id, event_id,  amount, currency) " +
        s"values (?, ?, ?, ?)"""

      val source = BatchSource.events(clientSettings)

      val ctrl = Projection.projectRows(
        source,
        events,
        Doobie.UpdateMany[CreatedRow](sql)) { envelope =>
        import envelope._

        event match {
          case Event(Created(ce)) =>
            val iou = IouContract.toIou(ce)
            List(CreatedRow(ce.contractId, ce.eventId, iou.data.amount, iou.data.currency))
          case _ => List()
        }
      }
      ctrl.cancel().map(_ mustBe Done)
    }

    "An experiment with Projection[CodegenType]" in {
      implicit val projector = Doobie.Projector()
      import IouContract._
      val projectionId = ProjectionId("my-id")
      val events =
        Projection[Iou.Contract](projectionId, ProjectionFilter.parties(Set(partyId)), projectionTable)
      case class CreatedRow(contractId: String, amount: BigDecimal, currency: String)
      val sql =
        s"""insert into ${events.table.name}(contract_id, event_id,  amount, currency) " +
        s"values (?, ?, ?, ?)"""

      val ctrl = Projection.projectRows(
        iouBatchSource(clientSettings),
        events,
        Doobie.UpdateMany[CreatedRow](sql)) { envelope =>
        val iou = envelope.unwrap
        List(CreatedRow(iou.id.contractId, iou.data.amount, iou.data.currency))
      }
      ctrl.cancel().map(_ mustBe Done)
    }
  }
}
