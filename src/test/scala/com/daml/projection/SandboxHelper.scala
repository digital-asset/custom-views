// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import java.io.File
import java.nio.file.Files
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.AkkaExecutionSequencerPool
import com.daml.ledger.api.domain.User
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.ledger.client.withoutledgerid.LedgerClient
import com.daml.ledger.resources.{ Resource, ResourceContext }
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.{ BridgeConfig, BridgeConfigAdaptor, SandboxOnXForTest, SandboxOnXRunner }
import com.daml.ledger.sandbox.SandboxOnXForTest._
import com.daml.lf.data.Ref.UserId
import com.daml.ports.Port
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{ BeforeAndAfterAll, Suite }

trait SandboxHelper extends BeforeAndAfterAll with StrictLogging {
  self: Suite =>
  implicit def system: ActorSystem
  implicit def resourceContext: ResourceContext = ResourceContext(system.dispatcher)
  implicit def ec = system.dispatcher
  implicit def esf = new AkkaExecutionSequencerPool(poolName = "sandbox-test", actorCount = 1)

  val timeout = 10.seconds
  val host = "127.0.0.1"
  private var resource: Resource[Port] = _
  var ledgerClient: LedgerClient = _
  var clientSettings: GrpcClientSettings = _
  implicit def grpcSettings = clientSettings
  var alicePartyId: String = _
  var bobPartyId: String = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val config = Default.copy(dataSource = Default.dataSource.map {
      case (pId, pdsc) => pId -> pdsc.copy(jdbcUrl = defaultH2SandboxJdbcUrl())
    })

    resource = SandboxOnXRunner.run(
      BridgeConfig.Default,
      config,
      new BridgeConfigAdaptor(),
      false
    ).acquire()
    val port = Await.result(resource.asFuture, timeout)
    ledgerClient = adminLedgerClient(port, config, None)
    loadTestDar(ledgerClient)
    val grpcSettings = GrpcClientSettings.connectToServiceAt(host, port.value).withTls(false)
    clientSettings = grpcSettings
  }

  def uniqueParty(name: String) = {
    def uniqueId() = java.util.UUID.randomUUID().toString
    val party = s"${name}_${uniqueId()}"
    val userId = party.toLowerCase
    val res = Await.result(
      ledgerClient.partyManagementClient.allocateParty(
        Some(party),
        Some(party),
        None
      ),
      timeout)
    Await.ready(
      ledgerClient.userManagementClient.createUser(User(UserId.assertFromString(userId), Some(res.party))),
      timeout)
    party
  }

  private def loadTestDar(ledgerClient: LedgerClient) = {
    val path = "target/daml"
    val files = new File(path).listFiles().toList
    Await.result(
      Future.sequence(files.map { darFile =>
        logger.info(s"Loading dar: ${darFile.getAbsolutePath}")
        val bytes = Files.readAllBytes(darFile.toPath)
        ledgerClient.packageManagementClient.uploadDarFile(ByteString.copyFrom(bytes))
      }),
      timeout
    )
  }

  private def adminLedgerClient(port: Port, config: Config, token: Option[String])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory
  ): LedgerClient = {
    val participant = config.participants(SandboxOnXForTest.ParticipantId)
    val sslContext = participant.apiServer.tls.flatMap(_.client())
    val clientConfig = LedgerClientConfiguration(
      applicationId = "admin-client",
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = token
    )
    LedgerClient.singleHost(
      hostIp = host,
      port = port.value,
      configuration = clientConfig,
      channelConfig = LedgerClientChannelConfiguration(sslContext)
    )
  }

  override protected def afterAll(): Unit = {
    ledgerClient.close()
    Await.ready(resource.release(), timeout)
    super.afterAll()
  }
}
