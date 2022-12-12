// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset

/**
 * Creates [[Offset]]s
 */
object Offset {
  private[projection] def protoOffset(offset: Option[Offset]) = offset.fold(begin)(mkProtoOffset)
  private val begin =
    LedgerOffset.defaultInstance.withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
  private def mkProtoOffset(offset: Offset) = LedgerOffset(
    LedgerOffset.Value.Absolute(offset.value)
  )
}

/**
 * An Offset on the Ledger.
 */
final case class Offset(value: String) {
  private[projection] def toProto = LedgerOffset(LedgerOffset.Value.Absolute(value))

  /**
   * Java API
   *
   * Gets the value
   */
  def getValue(): String = value
}

/**
 * Identifies a [[Projection]] process
 */
final case class ProjectionId(value: String) {

  /**
   * Java API
   *
   * Gets the value
   */
  def getValue(): String = value
}

/**
 * A SQL table that a [[Projection]] projects into
 */
final case class ProjectionTable(name: String) {

  /**
   * Java API
   *
   * Gets the name
   */
  def getName(): String = name
}
