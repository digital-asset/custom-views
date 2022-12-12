// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Keep, Source }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.daml.ledger.api.v1.event._
import com.daml.ledger.javaapi.data.{ CreatedEvent => JCreatedEvent }
import com.daml.quickstart.iou.iou._
import doobie._
import doobie.implicits._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must._
import org.scalatest.time._
import org.scalatest.wordspec._
import com.daml.ledger.api.v1.event.Event.Event.{ Archived, Created }
import com.daml.projection.scaladsl.{ Consumer, Control }

import scala.concurrent.Await
import scala.concurrent.duration._

class ProjectionSpec
    extends TestKit(ActorSystem("ProjectionSpec"))
    with AsyncWordSpecLike
    with GivenWhenThen
    with Matchers
    with BeforeAndAfterAll
    with EitherValues
    with ScalaFutures
    with OptionValues
    with TestEmbeddedPostgres
    with SandboxHelper {
  import TestUtil._

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(10, Millis)))

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  implicit val logHandler = LogHandler.nop
  type Action = JdbcAction

  "A Projection" must {
    "project created events up to endOffset and complete the stream" in {
      implicit val projector = JdbcProjector(ds)

      val alice = uniqueParty("Alice")

      Given("a created contract")
      val created = createIou(alice, alice, 100d).futureValue

      val projection = eventsProjection(alice)
      projector.getOffset(projection) must be(None)

      Given("a stream of created events")
      val source: Source[Batch[Event], Control] =
        Consumer
          .eventSource(
            clientSettings,
            projection.withEndOffset(Offset(created.completionOffset)).withBatchSize(2)
          )

      When("projecting created events")
      val p = Projection.fromCreated(insertCreateEvent)
      val projectionResults = source
        .via(
          Projection.Flows.project(projection, p))
        .via(projector.flow)

      val (res, probe) = projectionResults.toMat(TestSink.probe)(Keep.both).run()
      Then("a row is inserted into the projection table to initialize the projection")
      probe.request(1).expectNext() must be(1)

      Then("the create event for the first event should be projected")
      probe.request(1).expectNext() must be(1)

      Then("the projection should advance")
      probe.request(1).expectNext() must be(1)

      Then("the stream should complete")
      probe.request(1).expectComplete()

      val contractIds =
        runIO(sql"select contract_id from ious".query[String].to[List])

      Then("the projected table should contain the events")
      contractIds.size must be(1)

      Then("the projection has advanced to the tx offset associated to the first event")
      projector.getOffset(projection) must be(Some(Offset(created.completionOffset)))

      res.resourcesClosed.map(_ mustBe Done)
    }

    "project created events continuously" in {
      implicit val projector = JdbcProjector(ds)

      val party = uniqueParty("Bob")
      val projection = eventsProjection(party)
      projector.getOffset(projection) must be(None)

      Given("a created contract")
      val firstCreated = createIou(party, party, 100d).futureValue

      Given("a stream of created events")
      val source: Source[Batch[Event], Control] =
        Consumer
          .eventSource(clientSettings, projection.withBatchSize(1))

      When("projecting created events")
      val projectionResults = source
        .via(Projection.Flows.project(projection, Projection.fromCreated(insertCreateEvent)))
        .via(projector.flow)

      val (res, probe) = projectionResults.toMat(TestSink.probe)(Keep.both).run()

      Then("a row is inserted into the projection table to initialize the projection")
      probe.request(1).expectNext() must be(1)

      Then("the create event for the first event should be projected")
      probe.request(1).expectNext() must be(1)

      When("another contract is created")
      createIou(party, party, 110d).futureValue

      Then("the projection should advance")
      probe.request(1).expectNext() must be(1)

      Then("the create event for the second event should be projected")
      probe.request(1).expectNext() must be(1)

      probe.request(1).expectNoMessage()

      When("The test forces a rollback of transaction in progress")
      runIO(doobie.free.connection.rollback)

      val contractIds =
        runIO(sql"select contract_id from ious".query[String].to[List])

      Then("the projected table should contain the events")
      contractIds.size must be(1)

      Then("the projection has advanced to the tx offset associated to the first event")
      projector.getOffset(projection) must be(Some(Offset(firstCreated.completionOffset)))
      res.cancel().map(_ mustBe Done)
    }

    "project exercised events" in {
      implicit val projector = JdbcProjector(ds)

      val alice = uniqueParty("Alice")
      val bob = uniqueParty("Bob")

      Given("An exercised choice")
      val response = createAndExerciseIouTransfer(alice, alice, 100d, bob).futureValue

      Given("an ExercisedEvents projection")
      val projection =
        mkChoice(alice).withPredicate(choicePredicate(alice)).withEndOffset(Offset(response.completionOffset))
      projector.getOffset(projection) must be(None)

      When("projecting exercised events")
      val source =
        Consumer.exercisedEventSource(clientSettings, projection.withBatchSize(1))
      // This is just for testing, one insert per envelope is VERY SLOW (obviously).
      // see PerfSpec and Action.bulkFlow
      val toAction: Projection.Project[ExercisedEvent, Action] = { envelope =>
        import envelope._
        val actingParties = event.actingParties.mkString(",")
        val witnessParties = event.witnessParties.mkString(",")
        List(ExecuteUpdate(s"""insert into ${exercisedEventsProjectionTable.name}
          (contract_id, event_id, acting_parties, witness_parties, event_offset)
          values (:cid, :ap, :wp, :eid, :o)""")
          .bind("cid", event.contractId)
          .bind("ap", actingParties)
          .bind("wp", witnessParties)
          .bind("eid", event.eventId)
          .bind("o", offset.map(_.value)))
      }

      val projectionResults = source
        .via(Projection.Flows.project(projection, toAction))
        .via(projector.flow)

      Then("the expected events should be projected")
      val (res, probe) = projectionResults.toMat(TestSink.probe)(Keep.both).run()
      // insert into projection table (init)
      probe.request(1).expectNext() must be(1)

      // request event and tx boundary
      probe.request(1).expectNext() must be(1)
      // update projection
      probe.request(1).expectNext() must be(1)
      // completed
      probe.request(1).expectComplete()

      Then("the projection should have advanced to the offset associated to the event")
      projector.getOffset(projection) must be(Some(Offset(response.completionOffset)))

      val contractId =
        runIO(sql"select contract_id from exercised_events".query[String].to[List])
      Then("the projected table should contain the events")
      contractId.size must be(1)
      res.cancel().map(_ mustBe Done)
    }

    "continue from the projection after projecting events" in {
      implicit val projector = JdbcProjector(ds)
      val alice = uniqueParty("Alice")
      val projection = eventsProjection(alice)
      projector.getOffset(projection) must be(None)

      Given("a created contract")
      val created = createIou(alice, alice, 100d).futureValue

      Given("a stream of created events")
      val source: Source[Batch[Event], Control] =
        Consumer
          .eventSource(
            clientSettings,
            projection.withEndOffset(Offset(created.completionOffset)).withBatchSize(2)
          )

      When("projecting created events")
      val projectionResults = source
        .via(Projection.Flows.project(projection, Projection.fromCreated(insertCreateEvent)))
        .via(projector.flow)

      val (res, probe) = projectionResults.toMat(TestSink.probe)(Keep.both).run()
      Then("the projection table row is initialized")
      probe.request(1).expectNext() must be(1)

      Then("the create event for the first event should be projected")
      probe.request(1).expectNext() must be(1)

      Then("the projection should advance")
      probe.request(1).expectNext() must be(1)

      Then("the stream should complete")
      probe.request(1).expectComplete()

      // Stream completed
      Await.ready(res.resourcesClosed, 10.seconds)

      val contractIds =
        runIO(sql"select contract_id from ious".query[String].to[List])

      Then("the projected table should contain the events")
      contractIds.size must be(1)

      Then("the projection has advanced to the tx offset associated to the first event")
      val updatedOffset = projector.getOffset(projection)

      val updatedProjection = projection.withOffset(updatedOffset.value)
      updatedOffset.value must be(Offset(created.completionOffset))

      Given("another created contract")
      val createdNext = createIou(alice, alice, 110d).futureValue

      Given("streaming from the projection")
      val sourceFromProjection: Source[Batch[Event], Control] =
        Consumer
          .eventSource(
            clientSettings,
            updatedProjection.withEndOffset(Offset(createdNext.completionOffset)).withBatchSize(2)
          )

      When("projecting created events from projection")
      val projectionResultsFromProjection = sourceFromProjection
        .via(Projection.Flows.project(projection, Projection.fromCreated(insertCreateEvent)))
        .via(projector.flow)

      val (resNext, probeFromProjection) =
        projectionResultsFromProjection.toMat(TestSink.probe)(Keep.both).run()

      Then("projection table is already initialized")
      probeFromProjection.request(1).expectNext() must be(0)

      Then("the create event for the next event should be projected")
      probeFromProjection.request(1).expectNext() must be(1)

      Then("the projection should advance")
      probeFromProjection.request(1).expectNext() must be(1)

      Then("the stream should complete")
      probeFromProjection.request(1).expectComplete()

      // Stream closed
      Await.ready(resNext.resourcesClosed, 10.seconds)

      val contractIdsAfterResume =
        runIO(sql"select contract_id from ious".query[String].to[List])

      Then("the projected table should contain the events")
      contractIdsAfterResume.size must be(2)

      Then("the projection has advanced to the tx offset associated to the first event")
      val updatedOffsetAfter = projector.getOffset(projection)

      val updatedProjectionAfter = updatedProjection.withOffset(updatedOffsetAfter.value)
      updatedProjectionAfter.offset.value must be(Offset(createdNext.completionOffset))
    }

    "continue from the projection after projecting exercised events" in {
      implicit val projector = JdbcProjector(ds)
      val alice = uniqueParty("Alice")
      val bob = uniqueParty("Bob")

      Given("An exercised choice")
      val response = createAndExerciseIouTransfer(alice, alice, 100d, bob).futureValue

      Given("an ExercisedEvents.OfChoice projection")
      val projection = mkChoice(alice)
      projector.getOffset(projection) must be(None)

      When("projecting exercised events")
      val source =
        Consumer.exercisedEventSource(
          clientSettings,
          projection.withPredicate(choicePredicate(alice)).withEndOffset(
            Offset(response.completionOffset)).withBatchSize(1)
        )

      // This is just for testing, one insert per envelope is VERY SLOW (obviously).
      // see PerfSpec and Action.bulkFlow
      val toAction: Projection.Project[ExercisedEvent, Action] = { envelope =>
        import envelope._
        val actingParties = event.actingParties.mkString(",")
        val witnessParties = event.witnessParties.mkString(",")
        List(ExecuteUpdate(s"""insert into ${exercisedEventsProjectionTable.name}
          (contract_id, event_id, acting_parties, witness_parties, event_offset)
          values (:cid, :ap, :wp, :eid, :o)""")
          .bind("cid", event.contractId)
          .bind("ap", actingParties)
          .bind("wp", witnessParties)
          .bind("eid", event.eventId)
          .bind("o", offset.map(_.value)))
      }
      val projectionResults = source
        .via(Projection.Flows.project(projection, toAction))
        .via(projector.flow)

      Then("the expected events should be projected")
      val (res, probe) = projectionResults.toMat(TestSink.probe)(Keep.both).run()
      // projection table init
      probe.request(1).expectNext() must be(1)

      // request event and tx boundary
      probe.request(1).expectNext() must be(1)
      // update projection
      probe.request(1).expectNext() must be(1)
      probe.request(1).expectComplete()

      Then("the projection should have advanced to the offset associated to the event")
      val updatedOffset = projector.getOffset(projection)

      val projectionAfter = projection.withOffset(updatedOffset.value)
      Offset(response.completionOffset) must be(projectionAfter.offset.value)

      val contractId =
        runIO(sql"select contract_id from exercised_events".query[String].to[List])
      Then("the projected table should contain the events")
      contractId.size must be(1)
      // stream completed
      Await.ready(res.resourcesClosed, 10.seconds)

      Given("Another exercised choice")
      val responseAfter = createAndExerciseIouTransfer(alice, alice, 100d, bob).futureValue

      When("projecting exercised events from the advanced projection")

      val sourceAfter =
        Consumer.exercisedEventSource(
          clientSettings,
          projectionAfter.withPredicate(choicePredicate(alice)).withEndOffset(
            Offset(responseAfter.completionOffset)).withBatchSize(1)
        )

      val projectionResultsAfter = sourceAfter
        .via(Projection.Flows.project(projection, toAction))
        .via(projector.flow)

      Then("the expected events should be projected, after the first event")
      val (resAfter, probeAfter) = projectionResultsAfter.toMat(TestSink.probe)(Keep.both).run()
      // projection table is already initialized
      probeAfter.request(1).expectNext() must be(0)
      // request event and tx boundary
      probeAfter.request(1).expectNext() must be(1)
      // update projection
      probeAfter.request(1).expectNext() must be(1)
      probeAfter.request(1).expectComplete()

      Then(
        "the projection should have advanced to the offset associated to the last event"
      )
      val endOffset = projector.getOffset(projection)

      Offset(responseAfter.completionOffset) must be(endOffset.value)

      val contractIdsAfter =
        runIO(sql"select contract_id from exercised_events".query[String].to[List])
      Then("the projected table should contain the last event")
      contractIdsAfter.size must be(2)
      // stream completed
      resAfter.resourcesClosed.map(_ mustBe Done)
    }

    "project Iou.Contract with a BatchSource up to endOffset and complete the stream" in {
      implicit val projector = JdbcProjector(ds)
      import IouContract._

      val alice = uniqueParty("Alice")
      val projection = iouProjection(alice)
      projector.getOffset(projection) must be(None)

      Given("a created contract")
      val created = createIou(alice, alice, 100d).futureValue

      Given("a stream of created events")
      val source: Source[Batch[ContractAndEvent], Control] =
        batchSourceCAE(clientSettings)
          .src(
            projection.withEndOffset(Offset(created.completionOffset)).withBatchSize(2)
          )

      When("projecting created events")
      val insertIou: Projection.Project[ContractAndEvent, Action] = { envelope =>
        import envelope._
        val c = event
        val witnessParties = c.event.witnessParties.mkString(",")
        List(ExecuteUpdate(
          s"""insert into ${iousProjectionTable.name} (contract_id, event_id, witness_parties, amount, currency)
      values (:cid, :eid, :wp, :a, :c)""")
          .bind("cid", c.iou.id.contractId)
          .bind("eid", c.event.eventId)
          .bind("wp", witnessParties)
          .bind("a", c.iou.data.amount)
          .bind("c", c.iou.data.currency))
      }

      val projectionResults = source
        .via(
          Projection.Flows.project(projection, insertIou))
        .via(projector.flow)

      val (res, probe) = projectionResults.toMat(TestSink.probe)(Keep.both).run()
      Then("a row is inserted into the projection table to initialize the projection")
      probe.request(1).expectNext() must be(1)

      Then("the create event for the first event should be projected")
      probe.request(1).expectNext() must be(1)

      Then("the projection should advance")
      probe.request(1).expectNext() must be(1)

      Then("the stream should complete")
      probe.request(1).expectComplete()

      val contractIds =
        runIO(sql"select contract_id from ious".query[String].to[List])

      Then("the projected table should contain the events")
      contractIds.size must be(1)

      Then("the projection has advanced to the tx offset associated to the first event")
      projector.getOffset(projection) must be(Some(Offset(created.completionOffset)))
      res.resourcesClosed.map(_ mustBe Done)
    }
    "get batchSize from config if not defined" in {
      val alice = uniqueParty("Alice")
      val projection = eventsProjection(alice)
      Consumer.getBatchSize(projection) must be(system.settings.config.getInt("projection.batch-size"))
      Consumer.getBatchSize(projection.withBatchSize(10)) must be(10)
    }

    "select interface views given an interface ID" in {
      import com.daml.ledger.api.v1.transaction_filter.{ TransactionFilter, Filters, InclusiveFilters, InterfaceFilter }
      val alice = uniqueParty("Alice")
      val filter = ProjectionFilter.singleContractTypeId(Set(alice), templateId, isInterface = true)
      val ex = TransactionFilter(Map(alice -> Filters(Some(InclusiveFilters(
        Seq.empty,
        Seq(InterfaceFilter(Some(templateId), includeInterfaceView = true, includeCreateArgumentsBlob = false)))))))
      filter.transactionFilter must be(ex)
    }
  }

  def toIou(event: CreatedEvent) =
    Iou.COMPANION.fromCreatedEvent(JCreatedEvent.fromProto(CreatedEvent.toJavaProto(event)))

  val insertCreateEvent: Projection.Project[CreatedEvent, Action] = { envelope =>
    import envelope._
    val witnessParties = event.witnessParties.mkString(",")
    val iou = toIou(event)

    List(ExecuteUpdate(
      s"""insert into ${iousProjectionTable.name} (contract_id, event_id, witness_parties, amount, currency)
      values (?, ?, ?, ?, ?)""")
      .bind(1, event.contractId)
      .bind(2, event.eventId)
      .bind(3, witnessParties)
      .bind(4, iou.data.amount)
      .bind(5, iou.data.currency))
  }

  val CollectCreatedEvents = Flow[Batch[Event]].map {
    _.collect { case Event(Created(createdEvent)) =>
      createdEvent
    }
  }

  val CollectArchivedEvents = Flow[Batch[Event]].map {
    _.collect { case Event(Archived(archivedEvent)) =>
      archivedEvent
    }
  }
}
