// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import akka.actor.ActorSystem
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.v1.commands._
import doobie._
import doobie.implicits._
import cats.effect.unsafe.implicits.global
import com.daml.ledger.api.v1.value.{ List => _, Map => _, _ }
import com.daml.quickstart.model.iou._
import cats.effect.IO
import akka.grpc.GrpcClientSettings
import cats.implicits.catsSyntaxApplicativeError
import com.daml.ledger.api.v1.event.{ Event, ExercisedEvent }
import com.daml.ledger.javaapi.data.CommandsSubmission
import com.daml.ledger.rxjava.DamlLedgerClient
import com.daml.projection.IouContract.ContractAndEvent

object TestUtil {

  val templateId = Identifier(
    packageId = Iou.TEMPLATE_ID.getPackageId(),
    moduleName = Iou.TEMPLATE_ID.getModuleName(),
    entityName = Iou.TEMPLATE_ID.getEntityName()
  )

  val jTemplateId = com.daml.ledger.javaapi.data.Identifier.fromProto(Identifier.toJavaProto(templateId))

  val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())
  val iousProjectionTable = ProjectionTable("ious")

  def eventsProjection(partyId: String) = {
    Projection[Event](
      projectionId,
      ProjectionFilter.templateIdsByParty(Map(partyId -> Set(templateId)))
    )
  }
  def iouProjection(partyId: String) = {
    val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())

    Projection[ContractAndEvent](
      projectionId,
      ProjectionFilter.templateIdsByParty(Map(partyId -> Set(templateId)))
    )
  }

  val exercisedEventsProjectionTable = ProjectionTable("exercised_events")
  def mkChoice(partyId: String) = {
    val projectionId = ProjectionId(java.util.UUID.randomUUID().toString())

    Projection[ExercisedEvent](
      projectionId,
      ProjectionFilter.parties(Set(partyId))
    )
  }

  def choicePredicate(witnessParty: String): (Envelope[ExercisedEvent] => Boolean) = env => {
    val choice = "Iou_Transfer"
    env.event.choice == choice && env.event.witnessParties.contains(witnessParty)
  }

  def getOffset(projectionId: ProjectionId) = sql"""
      | select projection_offset
      |   from projection
      |  where id = ${projectionId}
      """.stripMargin.query[Offset].option.attempt

  def runIO[R](conIO: ConnectionIO[R])(implicit xa: Transactor[IO]) =
    conIO.transact(xa).unsafeRunSync()
  def createIou(issuer: String, owner: String, amount: Double)(implicit
      grpcSettings: GrpcClientSettings,
      sys: ActorSystem
  ): Future[SubmitAndWaitForTransactionIdResponse] = {
    val commandClient = CommandServiceClient(grpcSettings)

    val iou =
      new Iou(issuer, owner, "EUR", BigDecimal.decimal(amount).bigDecimal, List.empty.asJava)
    val commands = iou.create().commands().asScala.toList.map(c => Command.fromJavaProto(c.toProtoCommand))
    val submitRequest = SubmitAndWaitRequest(
      Some(
        Commands(
          party = issuer,
          applicationId = issuer,
          commandId = java.util.UUID.randomUUID.toString,
          submissionId = java.util.UUID.randomUUID.toString,
          commands = commands
        )
      )
    )
    commandClient.submitAndWaitForTransactionId(submitRequest)
  }

  def archive(party: String, contractId: String)(implicit
      grpcSettings: GrpcClientSettings,
      sys: ActorSystem
  ): Future[SubmitAndWaitForTransactionIdResponse] = {
    val commandClient = CommandServiceClient(grpcSettings)

    val cmd = Command().withExercise(
      ExerciseCommand(
        Some(templateId),
        contractId,
        "Archive",
        Some(Value(Value.Sum.Record(Record())))
      )
    )
    val submitRequest = SubmitAndWaitRequest(
      Some(
        Commands(
          party = party,
          applicationId = party,
          commandId = java.util.UUID.randomUUID.toString,
          submissionId = java.util.UUID.randomUUID.toString,
          commands = Seq(cmd)
        )
      )
    )
    commandClient.submitAndWaitForTransactionId(submitRequest)
  }

  def createAndExerciseIouTransferResult(issuer: String, owner: String, amount: Double, newOwner: String)(
      implicit grpcSettings: GrpcClientSettings): IouTransfer.ContractId = {
    val client = DamlLedgerClient.newBuilder(grpcSettings.serviceName, grpcSettings.defaultPort).build()
    client.connect()
    try {
      val commandId = java.util.UUID.randomUUID.toString
      val appId = issuer
      val iou =
        new Iou(issuer, owner, "EUR", BigDecimal.decimal(amount).bigDecimal, List.empty.asJava)
      val transfer = new Iou_Transfer(newOwner)
      val update = iou.createAnd().exerciseIou_Transfer(transfer)
      val actAs = List(owner).asJava
      val readAs = List(owner, newOwner).asJava
      val commandsSubmission =
        CommandsSubmission.create(appId, commandId, update.commands()).withActAs(actAs).withReadAs(readAs)
      client.getCommandClient.submitAndWaitForResult(
        commandsSubmission,
        update
      ).blockingGet().exerciseResult
    } finally client.close()
  }

  def createAndExerciseIouTransfer(issuer: String, owner: String, amount: Double, newOwner: String)(
      implicit
      grpcSettings: GrpcClientSettings,
      sys: ActorSystem
  ) = {
    val commandClient = CommandServiceClient(grpcSettings)
    val iou =
      new Iou(issuer, owner, "EUR", BigDecimal.decimal(amount).bigDecimal, List.empty.asJava)
    val transfer = new Iou_Transfer(newOwner)

    val commands = iou.createAnd().exerciseIou_Transfer(transfer).commands().asScala.toList.map(c =>
      Command.fromJavaProto(c.toProtoCommand))
    val submitRequest = SubmitAndWaitRequest(
      Some(
        Commands(
          party = issuer,
          applicationId = issuer,
          commandId = java.util.UUID.randomUUID.toString,
          commands = commands
        )
      )
    )
    commandClient.submitAndWaitForTransactionId(submitRequest)
  }
}
