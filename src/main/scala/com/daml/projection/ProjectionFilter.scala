// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import com.daml.ledger.api.v1.transaction_filter.{ Filters, InclusiveFilters, InterfaceFilter, TransactionFilter }
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.javaapi.data.{ codegen => jdc, Identifier => JIdentifier }

import java.{ util => ju }
import scala.jdk.CollectionConverters._

/** The rules that determine what ledger events are selected for a [[Projection]]. */
final class ProjectionFilter private (private[projection] val transactionFilter: TransactionFilter)

object ProjectionFilter {

  /** Java API */
  def templateIdsByParty(
      map: ju.Map[String, ju.Set[JIdentifier]]
  ): ProjectionFilter =
    templateIdsByParty(toScala(map))

  /**
   * Create a filter selecting events where each map key, a party, scans for events matching the template ID in the
   * associated set.
   */
  def templateIdsByParty(map: Map[String, Set[Identifier]]): ProjectionFilter =
    apply(mkTransactionFilter(map))

  /** Java API */
  def parties(partySet: ju.Set[String]): ProjectionFilter =
    parties(partySet.asScala.toSet)

  /** Create a filter selecting all events observable by any given party. */
  def parties(partySet: Set[String]): ProjectionFilter =
    apply(mkTransactionFilter(partySet.map(_ -> Set.empty[Identifier]).toMap))

  /**
   * Java API
   *
   * Filter for template payloads or interface views matching the given `contractType` companion, visible by any of
   * `partySet`.
   */
  def singleContractType(
      partySet: ju.Set[String],
      contractType: jdc.ContractTypeCompanion[_, _, _, _]): ProjectionFilter = {
    singleContractTypeId(
      partySet.asScala.toSet,
      Identifier.fromJavaProto(contractType.TEMPLATE_ID.toProto),
      contractType match {
        case _: jdc.ContractCompanion[_, _, _]  => false
        case _: jdc.InterfaceCompanion[_, _, _] => true
      }
    )
  }

  def singleContractTypeId(partySet: Set[String], contractType: Identifier, isInterface: Boolean): ProjectionFilter = {
    val (tids, ifs) = if (isInterface)
      (Seq.empty, Seq(InterfaceFilter(Some(contractType), includeInterfaceView = true)))
    else
      (Seq(contractType), Seq.empty)
    val filters = Filters(Some(InclusiveFilters(tids, ifs)))
    apply(TransactionFilter(partySet.view.map(_ -> filters).toMap))
  }

  private def mkTransactionFilter(templateIdsByParty: Map[String, Set[Identifier]]) =
    TransactionFilter(templateIdsByParty.map { case (name, ids) =>
      val filters = if (ids.isEmpty) Filters() else Filters(Some(InclusiveFilters(ids.toSeq)))
      name -> filters
    })

  private def toScala(templateIdsByParty: java.util.Map[String, java.util.Set[JIdentifier]]) =
    templateIdsByParty.asScala.toMap.transform { (_, v) =>
      v.asScala.map(i => Identifier.fromJavaProto(i.toProto)).toSet
    }

  private[this] def apply(tf: TransactionFilter): ProjectionFilter =
    new ProjectionFilter(tf)
}
