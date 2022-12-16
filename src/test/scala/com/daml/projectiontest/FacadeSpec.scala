// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projectiontest

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.event._
import com.daml.projection.{
  ExecuteUpdate,
  IouContract,
  JdbcAction,
  JdbcProjector,
  Projection,
  ProjectionFilter,
  ProjectionId,
  ProjectionTable,
  SandboxHelper,
  Sql,
  TestEmbeddedPostgres,
  UpdateMany
}
import com.daml.projection.scaladsl.BatchSource
import com.daml.quickstart.model.iou._
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

  val partyId = "alice"
  val projectionTable = ProjectionTable("ious")

  "A Projection" must {
    "provide an easy way to project events" in {
      import Projection._
      implicit val projector = JdbcProjector(ds)
      val projectionId = ProjectionId("my-id")
      val events = Projection[Event](projectionId, ProjectionFilter.parties(Set(partyId)))

      val insert: Project[CreatedEvent, JdbcAction] = { envelope =>
        import envelope._
        val witnessParties = event.witnessParties.mkString(",")
        val iou = IouContract.toIou(event)

        List(ExecuteUpdate(s"""insert into ${projectionTable.name}
          (contract_id, event_id, witness_parties, amount, currency)
          values (?, ?, ?, ?, ?)""")
          .bind(1, event.contractId)
          .bind(2, event.eventId)
          .bind(3, witnessParties)
          .bind(4, iou.data.amount)
          .bind(5, iou.data.currency))
      }

      val delete: Project[ArchivedEvent, JdbcAction] = { envelope =>
        import envelope._
        List(ExecuteUpdate(s"delete from ${projectionTable.name} where contract_id = :contract_id ").bind(
          "contract_id",
          event.contractId))
      }
      val f = Projection.fromCreatedOrArchived(insert, delete)
      implicit val source = BatchSource.events(clientSettings)
      val ctrl = Projection.project(source, events)(f)
      ctrl.cancel().map(_ mustBe Done)
    }

    "provide an easy way to project exercised events" in {
      implicit val projector = JdbcProjector(ds)

      val projectionId = ProjectionId("my-id")
      val projectionTable = ProjectionTable("contracts")
      val exercisedEvents =
        Projection[ExercisedEvent](projectionId, ProjectionFilter.parties(Set(partyId)))
      val source = BatchSource.exercisedEvents(clientSettings)

      val ctrl = Projection.project(source, exercisedEvents) {
        envelope =>
          import envelope._
          val actingParties = event.actingParties.mkString(",")
          val witnessParties = event.witnessParties.mkString(",")
          List[JdbcAction](ExecuteUpdate(s"""
          insert into ${projectionTable.name}
          (contract_id, event_id, acting_parties, witness_parties, event_offset)
          values ()
          """)
            .bind("contract_id", event.contractId)
            .bind("event_id", event.eventId)
            .bind("acting_parties", actingParties)
            .bind("witness_parties", witnessParties)
            .bind("event_offset", offset))
      }
      ctrl.cancel().map(_ mustBe Done)
    }

    "provide an easy way to project using a batch operation" in {
      implicit val projector = JdbcProjector(ds)

      val projectionId = ProjectionId("my-id")
      val events = Projection[Event](projectionId, ProjectionFilter.parties(Set(partyId)))
      case class CreatedRow(contractId: String, eventId: String, amount: BigDecimal, currency: String)

      val source = BatchSource.events(clientSettings)
      val updateMany = UpdateMany(
        Sql
          .binder[CreatedRow](s"""
          |insert into ${projectionTable.name} 
          |(
          |  contract_id, 
          |  event_id, 
          |  amount, 
          |  currency
          |) 
          |values (
          |  :contract_id, 
          |  :event_id, 
          |  :amount, 
          |  :currency
          |)""".stripMargin)
          .bind("contract_id", _.contractId)
          .bind("event_id", _.eventId)
          .bind("amount", _.amount)
          .bind("currency", _.currency)
      )

      val ctrl = Projection.projectRows(
        source,
        events,
        updateMany) { envelope =>
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
      implicit val projector = JdbcProjector(ds)

      import IouContract._
      val projectionId = ProjectionId("my-id")
      val events =
        Projection[Iou.Contract](projectionId, ProjectionFilter.parties(Set(partyId)))
      case class CreatedRow(contractId: String, amount: BigDecimal, currency: String)
      val updateMany = UpdateMany(
        Sql
          .binder[CreatedRow](s"""
          |insert into ${projectionTable.name} 
          |(
          |  contract_id, 
          |  event_id, 
          |  amount, 
          |  currency
          |) 
          |values (
          |  :contract_id, 
          |  :event_id, 
          |  :amount, 
          |  :currency
          |)""".stripMargin)
          .bind("contract_id", _.contractId)
          .bind("event_id", _ => "")
          .bind("amount", _.amount)
          .bind("currency", _.currency)
      )

      val ctrl = Projection.projectRows(
        iouBatchSource(clientSettings),
        events,
        updateMany) { envelope =>
        val iou = envelope.unwrap
        List(CreatedRow(iou.id.contractId, iou.data.amount, iou.data.currency))
      }
      ctrl.cancel().map(_ mustBe Done)
    }
  }
}
