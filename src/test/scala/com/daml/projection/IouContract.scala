// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import akka.grpc.GrpcClientSettings
import com.daml.ledger.api.v1.event._
import com.daml.ledger.javaapi.data.{ CreatedEvent => JCreatedEvent }
import com.daml.quickstart.iou.iou._
import com.daml.projection.scaladsl.BatchSource

object IouContract {
  case class ContractAndEvent(iou: Iou.Contract, event: CreatedEvent)

  def batchSourceCAE(clientSettings: GrpcClientSettings): BatchSource[ContractAndEvent] =
    BatchSource(clientSettings)(toContractAndEvent)

  def iouBatchSource(clientSettings: GrpcClientSettings): BatchSource[Iou.Contract] =
    BatchSource(clientSettings)(toIou)

  def toIou(event: CreatedEvent): Iou.Contract =
    Iou.Contract.fromCreatedEvent(
      JCreatedEvent.fromProto(CreatedEvent.toJavaProto(event))
    )
  def toContractAndEvent(event: CreatedEvent): ContractAndEvent =
    ContractAndEvent(toIou(event), event)
}
