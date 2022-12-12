// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.javadsl

import akka.Done
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.testkit.TestKit
import com.daml.ledger.javaapi.data.{ Unit => _, _ }
import com.daml.projection._
import com.daml.quickstart.iou.iou._
import doobie.implicits._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must._
import org.scalatest.time._
import org.scalatest.wordspec._
import org.scalatest.concurrent.Eventually.eventually

import java.sql.SQLException
import java.time.{ Instant, LocalDate, ZoneId }
import java.time.temporal.ChronoUnit
import java.util.{ List => JList, Optional }
import scala.jdk.CollectionConverters._
import scala.jdk.FunctionConverters._
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._

class JavaApiSpec
    extends TestKit(ActorSystem("JavaApiSpec"))
    with AnyWordSpecLike
    with GivenWhenThen
    with Matchers
    with EitherValues
    with ScalaFutures
    with OptionValues
    with TestEmbeddedPostgres
    with SandboxHelper {
  import TestUtil._

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(10, Millis)))

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  "Java API for projections" must {
    "project from a test batch source" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())

      Given("test batches")
      val size = 100
      val offsets = (0 until size).map(i => Offset(f"$i%07d"))

      val batch = Batch.create(
        offsets.map(o => mkIouEnvelope(o, mkIou(o, alice, alice), JList.of(alice))).asJava,
        TxBoundary.create[Event](projectionId, offsets.last)
      )
      val batchSource = BatchSource.create(
        JList.of(batch),
        BatchSource.GetContractTypeId.fromEvent(),
        BatchSource.GetParties.fromEvent())
      val projector = JdbcProjector.create(ds, system)
      val projectionTable = ProjectionTable("ious")
      val projection = Projection.create[Event](
        projectionId,
        ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
      )
      When("projecting created events")
      val control = projector.project(
        batchSource,
        projection.withEndOffset(offsets.last).withBatchSize(1),
        insertCreateEvent(projectionTable)
      )
      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done
      control.failed().asScala.failed.futureValue mustBe (an[NoSuchElementException])
      Then("the projected table should contain the events")
      val contractIds = runIO(sql"select contract_id from ious".query[String].to[List])
      contractIds.size must be(size)

      val amounts = runIO(sql"select amount, currency from ious".query[(Double, String)].to[List])
      amounts must contain theSameElementsAs ((0 until size).map(i => (i.toDouble, "EUR")))

      Then("the projection has advanced to the tx offset associated to the first event")
      projector.getCurrentOffset(projection).toScala must be(Some(offsets.last))
    }

    "project created events up to endOffset and stop automatically" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())
      val projectionTable = ProjectionTable("ious")

      Given("created contracts")
      createIou(alice, alice, 1d).futureValue
      createIou(alice, alice, 2d).futureValue
      val last = createIou(alice, alice, 3d).futureValue

      val projector = JdbcProjector.create(ds, system)

      val projection = Projection.create[Event](
        projectionId,
        ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
      )
      When("projecting created events")
      val control = projector.project(
        BatchSource.events(clientSettings),
        projection.withEndOffset(Offset(last.completionOffset)).withBatchSize(1),
        insertCreateEvent(projectionTable)
      )
      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done
      control.failed().asScala.failed.futureValue mustBe (an[NoSuchElementException])

      Then("the projected table should contain the events")
      val contractIds = runIO(sql"select contract_id from ious".query[String].to[List])
      contractIds.size must be(3)

      val amounts = runIO(sql"select amount, currency from ious".query[(Double, String)].to[List])
      amounts must contain theSameElementsAs (List((1d, "EUR"), (2d, "EUR"), (3d, "EUR")))

      Then("the projection has advanced to the tx offset associated to the first event")
      projector.getCurrentOffset(projection).toScala must be(Some(Offset(last.completionOffset)))
    }

    "project created events using a batch operation up to endOffset and stop automatically" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())
      val projectionTable = ProjectionTable("ious")

      Given("created contracts")
      createIou(alice, alice, 1d).futureValue
      createIou(alice, alice, 2d).futureValue
      val last = createIou(alice, alice, 3d).futureValue

      val projector = JdbcProjector.create(ds, system)

      val mkContract = { ce =>
        Iou.Contract.fromCreatedEvent(ce)
      }.asJava

      val projection = Projection.create[Iou.Contract](
        projectionId,
        ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
      )

      Given("An UpdateMany batch operation")
      val updateMany =
        UpdateMany.create(
          Sql
            .binder[Iou.Contract](s"""|insert into ${projectionTable.name} 
                |(
                |  contract_id, 
                |  event_id, 
                |  witness_parties, 
                |  amount, 
                |  currency
                |) 
                |values (
                |  :contract_id, 
                |  :event_id, 
                |  :witness_parties, 
                |  :amount, 
                |  :currency
                |)""".stripMargin)
            .bind("contract_id", _.id.contractId)
            .bind("event_id", _ => "")
            .bind("witness_parties", _ => "")
            .bind("amount", _.data.amount)
            .bind("currency", _.data.currency)
        )

      val fRows: Project[Iou.Contract, Iou.Contract] = { e: Envelope[Iou.Contract] =>
        List(e.unwrap).asJava
      }

      When("projecting created events")
      val control = projector.projectRows(
        BatchSource.create(clientSettings, mkContract),
        projection.withEndOffset(Offset(last.completionOffset)).withBatchSize(1),
        updateMany,
        fRows
      )

      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done
      control.failed().asScala.failed.futureValue mustBe (an[NoSuchElementException])

      Then("the projected table should contain the events")
      val contractIds = runIO(sql"select contract_id from ious".query[String].to[List])
      contractIds.size must be(3)

      val amounts = runIO(sql"select amount, currency from ious".query[(Double, String)].to[List])
      amounts must contain theSameElementsAs (List((1d, "EUR"), (2d, "EUR"), (3d, "EUR")))

      Then("the projection has advanced to the tx offset associated to the first event")
      projector.getCurrentOffset(projection).toScala must be(Some(Offset(last.completionOffset)))
    }

    "project contracts up to endOffset and stop automatically" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())
      val projectionTable = ProjectionTable("ious")

      Given("created contracts")
      createIou(alice, alice, 1d).futureValue
      createIou(alice, alice, 2d).futureValue
      val last = createIou(alice, alice, 3d).futureValue

      val projector = JdbcProjector.create(ds, system)

      val projection = Projection
        .create[Iou.Contract](
          projectionId,
          ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
        )
        .withEndOffset(Offset(last.completionOffset))

      When("projecting created events")

      val mkContract = { ce =>
        Iou.Contract.fromCreatedEvent(ce)
      }.asJava

      val control = projector.project[Iou.Contract](
        BatchSource.create(clientSettings, mkContract),
        projection.withBatchSize(1),
        insertIou(projectionTable)
      )
      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done

      Then("the projected table should contain the events")
      val contractIds = runIO(sql"select contract_id from ious".query[String].to[List])
      contractIds.size must be(3)

      val amounts = runIO(sql"select amount, currency from ious".query[(Double, String)].to[List])
      amounts must contain theSameElementsAs (List((1d, "EUR"), (2d, "EUR"), (3d, "EUR")))
      val witnesses = runIO(sql"select witness_parties from ious".query[Option[String]].to[List])
      witnesses(0) must be(None)

      Then("the projection has advanced to the tx offset associated to the first event")
      projector.getCurrentOffset(projection).toScala must be(Some(Offset(last.completionOffset)))
    }

    "project created and archived events" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())
      val projectionTable = ProjectionTable("ious")

      Given("created contracts")
      val created = createIou(alice, alice, 2d).futureValue

      val projector = JdbcProjector.create(ds, system)

      val projection = Projection
        .create[Event](
          projectionId,
          ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
        )
        .withEndOffset(Offset(created.completionOffset))

      When("projecting events")
      val control = projector.project(
        BatchSource.events(clientSettings),
        projection.withBatchSize(1),
        insertOrDelete(projectionTable)
      )

      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done

      Then("the projected table should contain the events")
      val contractIds = runIO(sql"select contract_id from ious".query[String].to[List])
      contractIds.size must be(1)
      archive(alice, contractIds(0)).futureValue
      val createdAfterArchive = createIou(alice, alice, 2d).futureValue

      val controlAfter = projector.project(
        BatchSource.events(clientSettings),
        projection.withEndOffset(Offset(createdAfterArchive.completionOffset)).withBatchSize(1),
        insertOrDelete(projectionTable)
      )
      // projecting up to an offset, when it is reached the projection stops automatically
      controlAfter.completed().asScala.futureValue mustBe Done
      val contractIdsAfter = runIO(sql"select contract_id from ious".query[String].to[List])
      // one archived, one created
      contractIdsAfter.size must be(1)
      val amounts = runIO(sql"select amount, currency from ious".query[(Double, String)].to[List])
      amounts(0) must be((2d, "EUR"))

      Then("the projection has advanced to the tx offset associated to the archived event")
      projector.getCurrentOffset(projection).toScala must be(
        Some(Offset(createdAfterArchive.completionOffset))
      )
    }

    "project created and archived events continuously" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())
      val projectionTable = ProjectionTable("ious")

      Given("created contracts")
      createIou(alice, alice, 2d).futureValue

      val projector = JdbcProjector.create(ds, system)

      val projection = Projection.create[Event](
        projectionId,
        ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
      )

      When("projecting events")
      val control =
        projector.project(BatchSource.events(clientSettings), projection, insertOrDelete(projectionTable))

      Then("eventually the projected table should contain the events")
      eventually(timeout(Span(5, Seconds))) {
        val contractIds = runIO(sql"select contract_id from ious".query[String].to[List])
        contractIds.size must be(1)
      }
      control.cancel().asScala.futureValue
      control.completed().asScala.futureValue mustBe Done
    }

    "project exercised events" in {
      val alice = uniqueParty("Alice")
      val bob = uniqueParty("Bob")

      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())
      val projectionTable = ProjectionTable("java_api_exercised_events")

      Given("Exercised choices")
      val firstResultContractId =
        createAndExerciseIouTransferResult(alice, alice, 100d, bob).contractId
      val exercised = createAndExerciseIouTransfer(alice, alice, 100d, bob).futureValue

      def choice(witnessParty: String) = { env: Envelope[ExercisedEvent] =>
        val choice = Iou.CHOICE_Iou_Transfer.name
        env.event.getChoice == choice && env.event.getWitnessParties.contains(witnessParty)
      }.asJava

      Given("an ExercisedEvents projection")
      val projection = Projection[ExercisedEvent](
        projectionId,
        ProjectionFilter.parties(Set(alice))
      ).withPredicate(choice(alice))

      val projector = JdbcProjector.create(ds, system)
      val control = projector.project(
        BatchSource.exercisedEvents(clientSettings),
        projection.withEndOffset(Offset(exercised.completionOffset)).withBatchSize(1),
        insertExercisedTransfer(projectionTable)
      )

      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done
      val contractIds =
        runIO(
          sql"select transfer_result_contract_id from java_api_exercised_events"
            .query[String]
            .to[List]
        )
      Then("the projected table should contain the events")
      contractIds.size must be(2)
      contractIds must contain(firstResultContractId)

      Then("the projection has advanced to the tx offset associated to the archived event")
      projector.getCurrentOffset(projection).toScala must be(
        Some(Offset(exercised.completionOffset))
      )
    }

    "continue from the projection after projecting events" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())
      val projectionTable = ProjectionTable("ious")

      Given("created contracts")
      val created = createIou(alice, alice, 1d).futureValue

      val projector = JdbcProjector.create(ds, system)

      val projection = Projection.create[Event](
        projectionId,
        ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
      )

      When("projecting events")
      val control = projector.project(
        BatchSource.events(clientSettings),
        projection.withEndOffset(Offset(created.completionOffset)).withBatchSize(1),
        insertOrDelete(projectionTable)
      )

      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done
      val contractIds = runIO(sql"select contract_id from ious".query[String].to[List])
      // one archived, one created
      contractIds.size must be(1)
      val amounts = runIO(sql"select amount, currency from ious".query[(Double, String)].to[List])
      amounts(0) must be((1d, "EUR"))

      Then("the projection has advanced to the tx offset associated to the archived event")
      projector.getCurrentOffset(projection).toScala must be(Some(Offset(created.completionOffset)))

      val createdNext = createIou(alice, alice, 2d).futureValue

      val controlAfter = projector.project(
        BatchSource.events(clientSettings),
        projection.withEndOffset(Offset(createdNext.completionOffset)).withBatchSize(1),
        insertOrDelete(projectionTable)
      )

      controlAfter.completed().asScala.futureValue mustBe Done

      val contractIdsAfter = runIO(sql"select contract_id from ious".query[String].to[List])
      // one archived, one created
      contractIdsAfter.size must be(2)
      val amountsAfter =
        runIO(
          sql"select amount, currency from ious order by amount asc"
            .query[(Double, String)]
            .to[List]
        )
      amountsAfter must be(List((1d, "EUR"), (2d, "EUR")))

      Then("the projection has advanced to the tx offset associated to the archived event")
      projector.getCurrentOffset(projection).toScala must be(
        Some(Offset(createdNext.completionOffset))
      )
    }

    "project tree events" in {
      val alice = uniqueParty("Alice")
      val bob = uniqueParty("Bob")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())
      val projectionTable = ProjectionTable("java_api_tree_events")

      Given("Exercised choices")
      val firstResultContractId =
        createAndExerciseIouTransferResult(alice, alice, 100d, bob).contractId
      val exercised = createAndExerciseIouTransfer(alice, alice, 100d, bob).futureValue

      val projector = JdbcProjector.create(ds, system)

      val projection = Projection.create[TreeEvent](
        projectionId,
        ProjectionFilter.parties(Set(alice).asJava)
      )

      When("projecting events")
      val control = projector.project(
        BatchSource.treeEvents(clientSettings),
        projection.withEndOffset(Offset(exercised.completionOffset)).withBatchSize(1),
        insertTreeTransfer(projectionTable)
      )
      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done

      val transferResultContractIds =
        runIO(
          sql"select transfer_result_contract_id from java_api_tree_events".query[String].to[List]
        )
      Then("the projected table should contain the events")
      // create + exercise
      transferResultContractIds.size must be(4)
      transferResultContractIds must contain(firstResultContractId)
      transferResultContractIds.filter(_ == "").size must be(2)

      Then("the projection has advanced to the tx offset associated to the archived event")
      projector.getCurrentOffset(projection).toScala must be(
        Some(Offset(exercised.completionOffset))
      )
    }

    "stop the projection process and fail when an exception occurs in a JdbcAction" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())

      Given("created contracts")
      createIou(alice, alice, 1d).futureValue

      val projector = JdbcProjector.create(ds, system)

      val projection = Projection.create[Event](
        projectionId,
        ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
      )
      val failingSql: Project[Event, JdbcAction] = { _ =>
        JList.of(ExecuteUpdate("insert into blabla"))
      }
      When("projecting created events with an illegal SQL statement")
      val control = projector.project(
        BatchSource.events(clientSettings),
        projection.withBatchSize(1),
        failingSql
      )
      Then("the projection should fail")
      // Resources should automatically close on a failed projection
      control.failed().asScala.futureValue mustBe (an[SQLException])
      control.completed().asScala.futureValue mustBe Done
      // Cancelling a projection that failed should succeed
      control.cancel().asScala.futureValue mustBe Done
    }

    "stop the projection process and fail when using bad grpc settings" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())

      val projector = JdbcProjector.create(ds, system)

      val projection = Projection.create[Event](
        projectionId,
        ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
      )
      val failingSql: Project[Event, JdbcAction] = { _ =>
        JList.of(ExecuteUpdate.create("insert into blabla"))
      }
      When("projecting created events from a non-existent ledger")
      val badGrpcSettings = GrpcClientSettings.connectToServiceAt("bla", 8081)
      val control = projector.project(
        BatchSource.events(badGrpcSettings),
        projection.withBatchSize(1),
        failingSql
      )

      Then("the projection should fail")
      // Resources should automatically close on a failed projection
      control.failed().asScala.futureValue mustBe (an[io.grpc.StatusRuntimeException])
      control.completed().asScala.futureValue mustBe Done
      // Cancelling a projection that failed should succeed
      control.cancel().asScala.futureValue mustBe Done
    }
    "project ledger data visible by other db sessions according to txs on ledger" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())

      Given("test batches, half with tx boundaries, half without")
      val size = 100

      val batches = (0 until size).map { i =>
        // offset value is compared as strings, sorted alphabetically
        val offset = Offset(f"$i%07d")
        if (i < size / 2) {
          Batch.create(
            List(mkIouEnvelope(offset, mkIou(offset, alice, alice), JList.of(alice))).asJava,
            TxBoundary.create[Event](projectionId, offset)
          )
        } else {
          Batch.create(
            List(mkIouEnvelope(offset, mkIou(offset, alice, alice), JList.of(alice))).asJava
          )
        }
      }
      val batchSource = BatchSource.create(
        batches.asJava,
        BatchSource.GetContractTypeId.fromEvent(),
        BatchSource.GetParties.fromEvent())
      val projector = JdbcProjector.create(ds, system)
      val projectionTable = ProjectionTable("ious")
      val projection = Projection.create[Event](
        projectionId,
        ProjectionFilter.templateIdsByParty(Map(alice -> Set(jTemplateId).asJava).asJava)
      )
      When("projecting created events")
      val control = projector.project(
        batchSource,
        projection.withEndOffset(Offset(f"${size - 1}%07d")),
        insertCreateEvent(projectionTable)
      )
      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done
      control.failed().asScala.failed.futureValue mustBe (an[NoSuchElementException])

      Then("the projected table should only contain events committed on the ledger")
      val contractIds = runIO(sql"select contract_id from ious".query[String].to[List])
      contractIds.size must be(size / 2)

      Then("the projection has advanced to the last tx boundary offset")
      projector.getCurrentOffset(projection).toScala must be(
        batches(size / 2 - 1).boundary.map(_.offset)
      )
    }

    "project supported column datatypes" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())

      Given("test batches")
      val size = 3
      val offsets = (0 until size).map(i => Offset(f"$i%07d"))
      val records = offsets.map(o => mkEnvelope(o, mkRecord(o, alice)))
      val batch = Batch.create(
        records.asJava,
        TxBoundary.create[Record](projectionId, offsets.last)
      )
      val batchSource =
        BatchSource.create(
          JList.of(batch),
          (_: Record) => Optional.empty[Identifier](),
          (_: Record) => Set.empty[String].asJava)
      val projector = JdbcProjector.create(ds, system)
      val projectionTable = ProjectionTable("column_types_table")
      val projection = Projection.create[Record](
        projectionId,
        ProjectionFilter.parties(Set(alice))
      )
      When("projecting records")
      val control = projector.project(
        batchSource,
        projection.withEndOffset(offsets.last).withBatchSize(1),
        insertRecord(projectionTable)
      )
      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done
      control.failed().asScala.failed.futureValue mustBe (an[NoSuchElementException])
      Then("the projected table should contain the records")
      import doobie.postgres.implicits._
      val recordsDb = runIO(sql"select * from column_types_table".query[Record].to[List])
      recordsDb.size must be(size)
      recordsDb must contain theSameElementsAs records.map(_.event)

      Then("the projection has advanced to the tx offset associated to the first event")
      projector.getCurrentOffset(projection).toScala must be(Some(offsets.last))
    }

    "project records, setting supported column datatypes to NULL" in {
      val alice = uniqueParty("Alice")
      val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())

      Given("test batches")
      val size = 3
      val offsets = (0 until size).map(i => Offset(f"$i%07d"))
      val records = offsets.map(o => mkEnvelope(o, mkNoneRecord(alice)))
      val batch = Batch.create(
        records.asJava,
        TxBoundary.create[Record](projectionId, offsets.last)
      )
      val batchSource =
        BatchSource.create(
          JList.of(batch),
          (_: Record) => Optional.empty[Identifier](),
          (_: Record) => Set.empty[String].asJava)
      val projector = JdbcProjector.create(ds, system)
      val projectionTable = ProjectionTable("column_types_table")
      val projection = Projection.create[Record](
        projectionId,
        ProjectionFilter.parties(Set(alice))
      )
      When("projecting records")
      val control = projector.project(
        batchSource,
        projection.withEndOffset(offsets.last).withBatchSize(1),
        insertRecord(projectionTable)
      )
      // projecting up to an offset, when it is reached the projection stops automatically
      control.completed().asScala.futureValue mustBe Done
      control.failed().asScala.failed.futureValue mustBe (an[NoSuchElementException])
      Then("the projected table should contain the records")
      import doobie.postgres.implicits._
      val recordsDb = runIO(sql"select * from column_types_table".query[Record].to[List])
      recordsDb.size must be(size)
      recordsDb must contain theSameElementsAs records.map(_.event)

      Then("the projection has advanced to the tx offset associated to the first event")
      projector.getCurrentOffset(projection).toScala must be(Some(offsets.last))
    }
  }
  case class Record(
      contract_id: Option[String],
      stakeholder: String,
      long_column: Option[Long],
      int_column: Option[Int],
      short_column: Option[Short],
      boolean_column: Option[Boolean],
      double_column: Option[Double],
      float_column: Option[Float],
      offset: Option[Offset],
      projection_id: Option[ProjectionId],
      bigdecimal: Option[BigDecimal],
      date: Option[LocalDate],
      timestamp: Option[Instant]
  )
  // postgres timestamp does not support nanos ootb
  def mkRecord(offset: Offset, stakeholder: String): Record = {
    val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
    val zoneId = ZoneId.of("UTC")
    Record(
      contract_id = Some("contract_id"),
      stakeholder = stakeholder,
      long_column = Some(1L),
      int_column = Some(1),
      short_column = Some(2),
      boolean_column = Some(false),
      double_column = Some(1.3d),
      float_column = Some(1.2f),
      offset = Some(offset),
      projection_id = Some(ProjectionId("id")),
      bigdecimal = Some(BigDecimal.decimal(100d)),
      date = Some(LocalDate.ofInstant(now, zoneId)),
      timestamp = Some(now)
    )
  }
  def mkNoneRecord(stakeholder: String): Record = {
    Record(
      contract_id = None,
      stakeholder = stakeholder,
      long_column = None,
      int_column = None,
      short_column = None,
      boolean_column = None,
      double_column = None,
      float_column = None,
      offset = None,
      projection_id = None,
      bigdecimal = None,
      date = None,
      timestamp = None
    )
  }

  def insertRecord(table: ProjectionTable): Project[Record, JdbcAction] = { envelope: Envelope[Record] =>
    import envelope._
    val record = unwrap
    JList.of(
      ExecuteUpdate
        .create(
          s"""|insert into ${table.name} 
              |( 
              |  contract_id,
              |  stakeholder,
              |  long_column,
              |  int_column,
              |  short_column,
              |  boolean_column,
              |  double_column,
              |  float_column,
              |  event_offset,
              |  projection_id,
              |  bigdecimal_column,
              |  date_column,
              |  timestamp_column
              |) 
              |values (
              |  :contract_id,
              |  :stakeholder,
              |  :long_column,
              |  :int_column,
              |  :short_column,
              |  :boolean_column,
              |  :double_column,
              |  :float_column,
              |  :offset,
              |  :projection_id,
              |  :bigdecimal_column,
              |  :date_column,
              |  :timestamp_column
              |)
              |""".stripMargin)
        .bind("contract_id", record.contract_id)
        .bind("stakeholder", record.stakeholder)
        .bind("long_column", record.long_column)
        .bind("int_column", record.int_column)
        .bind("short_column", record.short_column)
        .bind("boolean_column", record.boolean_column)
        .bind("double_column", record.double_column)
        .bind("float_column", record.float_column)
        .bind("offset", record.offset.map(_.value))
        .bind("projection_id", record.projection_id)
        .bind("bigdecimal_column", record.bigdecimal)
        .bind("date_column", record.date)
        .bind("timestamp_column", record.timestamp)
    )
  }

  val iousProjectionTable = ProjectionTable("ious")

  def mkIou(offset: Offset, issuer: String, owner: String) =
    new Iou.Contract(
      new Iou.ContractId(offset.toString),
      new Iou(
        issuer,
        owner,
        "EUR",
        BigDecimal.decimal(offset.value.toDouble).bigDecimal,
        JList.of()
      ),
      None.toJava,
      Set(issuer, owner).asJava,
      Set(issuer, owner).asJava
    )

  def mkIouEnvelope(offset: Offset, iou: Iou.Contract, witnessParties: JList[String]) = {
    Envelope.create[Event](
      new CreatedEvent(
        witnessParties,
        offset.value,
        iou.getContractTypeId,
        "contract-id",
        iou.data.toValue,
        java.util.Map.of(),
        java.util.Map.of(),
        None.toJava,
        None.toJava,
        iou.signatories,
        iou.observers
      ),
      None.toJava,
      None.toJava,
      None.toJava,
      Some(offset).toJava
    )
  }

  def mkEnvelope[T](offset: Offset, t: T) = {
    Envelope.create[T](
      t,
      None.toJava,
      None.toJava,
      None.toJava,
      Some(offset).toJava
    )
  }

  def insertCreateEvent(table: ProjectionTable): Project[Event, JdbcAction] = { envelope: Envelope[Event] =>
    import envelope._
    val witnessParties = event.getWitnessParties().asScala.toList.mkString(",")
    // Java usage
    if (event.isInstanceOf[CreatedEvent]) {
      val iou = Iou.Contract.fromCreatedEvent(event.asInstanceOf[CreatedEvent])
      JList.of(
        ExecuteUpdate
          .create(s"""|insert into ${table.name} 
              |( 
              |  contract_id, 
              |  event_id, 
              |  witness_parties, 
              |  amount, 
              |  currency
              |) 
              |values (
              |  :contract_id, 
              |  :event_id,
              |  :witness_parties,
              |  :amount,
              |  :currency
              |)
              |""".stripMargin)
          .bind("contract_id", event.getContractId())
          .bind("event_id", event.getEventId())
          .bind("witness_parties", witnessParties)
          .bind("amount", iou.data.amount)
          .bind("currency", iou.data.currency)
      )
    } else {
      JList.of()
    }
  }

  def insertIou(table: ProjectionTable): Project[Iou.Contract, JdbcAction] = { envelope: Envelope[Iou.Contract] =>
    import envelope._
    val contract = unwrap
    val iou = contract.data

    JList.of(
      ExecuteUpdate
        .create(
          s"""insert into ${table.name} (contract_id, event_id, witness_parties, amount, currency) values (?, ?, ?, ?, ?)"""
        )
        .bind(1, contract.id.contractId)
        .bind(2, "")
        .bind(3, null: String)
        .bind(4, iou.amount)
        .bind(5, iou.currency)
    )
  }

  def insertOrDelete(table: ProjectionTable): Project[Event, JdbcAction] = { envelope: Envelope[Event] =>
    import envelope._
    val witnessParties = event.getWitnessParties().asScala.toList.mkString(",")
    // Java usage
    if (event.isInstanceOf[CreatedEvent]) {
      val iou = Iou.Contract.fromCreatedEvent(event.asInstanceOf[CreatedEvent])

      JList.of(
        ExecuteUpdate(
          s"""insert into ${table.name} (contract_id, event_id, witness_parties, amount, currency) values (?, ?, ?, ?, ?)"""
        )
          .bind(1, event.getContractId())
          .bind(2, event.getEventId())
          .bind(3, witnessParties)
          .bind(4, iou.data.amount)
          .bind(5, iou.data.currency)
      )
    } else if (event.isInstanceOf[ArchivedEvent]) {
      val archivedEvent = event.asInstanceOf[ArchivedEvent]
      JList.of[JdbcAction](
        ExecuteUpdate(s"""delete from ${iousProjectionTable.name} where contract_id = ?""")
          .bind(1, archivedEvent.getContractId())
      )
    } else {
      JList.of()
    }
  }

  def insertExercisedTransfer(table: ProjectionTable): Project[ExercisedEvent, JdbcAction] = {
    envelope: Envelope[ExercisedEvent] =>
      import envelope._
      val actingParties = event.getActingParties.asScala.mkString(",")
      val witnessParties = event.getWitnessParties.asScala.mkString(",")
      if (event.getChoice == Iou.CHOICE_Iou_Transfer.name) {
        event.getExerciseResult
          .asContractId()
          .toScala
          .map { contractId =>
            JList.of[JdbcAction](
              ExecuteUpdate(s"""insert into ${table.name}
          (contract_id, transfer_result_contract_id, event_id, acting_parties, witness_parties, event_offset)
          values (?, ?, ?, ?, ?, ?)""")
                .bind(1, event.getContractId)
                .bind(2, contractId.getValue)
                .bind(3, actingParties)
                .bind(4, witnessParties)
                .bind(5, event.getEventId)
                .bind(6, offset.map(_.value).getOrElse(""))
            )
          }
          .getOrElse(JList.of())
      } else JList.of()
  }

  def insertTreeTransfer(table: ProjectionTable): Project[TreeEvent, JdbcAction] = { envelope: Envelope[TreeEvent] =>
    import envelope._
    val witnessParties = event.getWitnessParties.asScala.mkString(",")
    event match {
      case created: CreatedEvent =>
        JList.of[JdbcAction](
          ExecuteUpdate(s"""|insert into ${table.name}
              |(
              |  contract_id,
              |  transfer_result_contract_id,
              |  event_id,
              |  acting_parties,
              |  witness_parties,
              |  event_offset
              |)
              |values (:contract_id,
              |  :transfer_result_contract_id,
              |  :event_id,
              |  :acting_parties,
              |  :witness_parties,
              |  :event_offset
              |)""".stripMargin)
            .bind("contract_id", created.getContractId)
            .bind("transfer_result_contract_id", "")
            .bind("event_id", created.getEventId)
            .bind("acting_parties", "")
            .bind("witness_parties", witnessParties)
            .bind("event_offset", offset.map(_.value).getOrElse(""))
        )

      case exercised: ExercisedEvent =>
        if (exercised.getChoice == Iou.CHOICE_Iou_Transfer.name) {
          val actingParties = exercised.getActingParties.asScala.mkString(",")
          exercised.getExerciseResult
            .asContractId()
            .toScala
            .map { contractId =>
              JList.of[JdbcAction](
                ExecuteUpdate(s"""insert into ${table.name}
          (contract_id, transfer_result_contract_id, event_id, acting_parties, witness_parties, event_offset)
          values (?, ?, ?, ?, ?, ?)""")
                  .bind(1, exercised.getContractId)
                  .bind(2, contractId.getValue)
                  .bind(3, actingParties)
                  .bind(4, witnessParties)
                  .bind(5, exercised.getEventId)
                  .bind(6, offset.map(_.value).getOrElse(""))
              )
            }
            .getOrElse(JList.of())
        } else JList.of()
    }
  }
}
