// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection.scaladsl

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }
import akka.stream.{ KillSwitch, KillSwitches }
import akka.{ Done, NotUsed }
import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceClient,
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.daml.ledger.api.v1.event.{ Event, ExercisedEvent }
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  GetTransactionsResponse,
  TransactionServiceClient
}
import com.daml.projection.{ Batch, Batcher, ConsumerRecord, Envelope, Offset, Projection, ProjectionId, TxBoundary }
import com.daml.projection.Projection._

import java.time.Instant
import scala.concurrent.{ ExecutionContext, Future, Promise }
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

private[projection] object Consumer extends StrictLogging {
  def eventSource(
      clientSettings: GrpcClientSettings,
      projection: Projection[Event]
  )(implicit sys: ActorSystem): Source[Batch[Event], Control] = {
    implicit val ec = sys.dispatcher
    val predicate = projection.predicate
    val client = ActiveContractsServiceClient(clientSettings)
    val txClient = TransactionServiceClient(clientSettings)
    def createControl(killSwitch: KillSwitch) = GrpcControl(List(client, txClient), killSwitch)
    def completeActiveContractSource = Flow[Batch[Event]].viaMat(
      completeWithControl(
        projection,
        { () =>
          GrpcControl.closeClients(List(client, txClient))
        },
        createControl
      )
    )(Keep.right)

    // If the projection has an offset, the ACS can be skipped, since it was already handled before.
    // The active contract set is only requested if the projection has no offset.
    if (projection.offset.isEmpty) {
      logger.trace(s"Getting the ACS for projection: '${projection.id}'")
      val source = client.getActiveContracts(
        GetActiveContractsRequest(
          filter = Some(projection.transactionFilter)
        )
      )
      activeContractSourceFromTxs(
        source,
        (offset, transactionFilter) =>
          getTransactions(txClient, projection.endOffset, offset, transactionFilter),
        projection,
        predicate,
        getBatchSize(projection),
        getDefaultBatcherInterval()
      )
        .viaMat(completeActiveContractSource)(Keep.right)

    } else {
      logger.trace(s"Getting transactions for projection: '${projection.id}' from offset '${projection.offset}'.")
      getTransactions(txClient, projection.endOffset, projection.offset, projection.transactionFilter)
        .via(toConsumerRecord(projection.id, predicate))
        .via(Batcher[Event](getBatchSize(projection), getDefaultBatcherInterval()))
        .viaMat(completeActiveContractSource)(Keep.right)
    }
  }

  private[projection] def getBatchSize(projection: Projection[_])(implicit sys: ActorSystem) = {
    projection.batchSize.getOrElse(sys.settings.config.getInt("projection.batch-size"))
  }

  private[projection] def getDefaultBatcherInterval()(implicit sys: ActorSystem): FiniteDuration = {
    import scala.concurrent.duration._
    FiniteDuration(sys.settings.config.getDuration("projection.batch-interval").toNanos, TimeUnit.NANOSECONDS)
  }

  def exercisedEventSource(
      clientSettings: GrpcClientSettings,
      projection: Projection[ExercisedEvent]
  )(implicit sys: ActorSystem): Source[Batch[ExercisedEvent], Control] = {
    implicit val ec = sys.dispatcher

    val predicate = projection.predicate
    val client = TransactionServiceClient(clientSettings)
    def createControl(killSwitch: KillSwitch) = GrpcControl(List(client), killSwitch)
    exercisedEventSourceFromTrees(
      getTransactionTrees(client, projection.endOffset, projection.offset, projection.transactionFilter),
      projection,
      predicate,
      getBatchSize(projection),
      getDefaultBatcherInterval()
    )
      .viaMat(completeTreeEventSource(projection, client, createControl))(Keep.right)
  }

  def treeEventSource(
      clientSettings: GrpcClientSettings,
      projection: Projection[TreeEvent]
  )(implicit sys: ActorSystem): Source[Batch[TreeEvent], Control] = {
    implicit val ec = sys.dispatcher
    val predicate = projection.predicate

    val client = TransactionServiceClient(clientSettings)
    def createControl(killSwitch: KillSwitch) = GrpcControl(List(client), killSwitch)

    treeEventSourceFromTrees(
      getTransactionTrees(client, projection.endOffset, projection.offset, projection.transactionFilter),
      projection.id,
      predicate,
      getBatchSize(projection),
      getDefaultBatcherInterval()
    )
      .viaMat(completeTreeEventSource(projection, client, createControl))(Keep.right)
  }

  private def completeTreeEventSource[T](
      projection: Projection[T],
      client: TransactionServiceClient,
      createControl: KillSwitch => Control
  )(implicit ec: ExecutionContext) =
    completeWithControl[T](
      projection,
      { () =>
        client.close().flatMap(_ => client.closed)
      },
      createControl
    )

  private def completeWithControl[T](
      projection: Projection[T],
      closeResources: () => Future[Done],
      createControl: KillSwitch => Control
  ) = {
    val controlRef = new java.util.concurrent.atomic.AtomicReference[Control]()
    Flow[Batch[T]]
      .viaMat(KillSwitches.single)(Keep.right)
      .mapMaterializedValue { switch =>
        val control = createControl(switch)
        controlRef.set(control)
        control
      }
      .alsoTo(
        Flow[Any]
          .fold(())(Keep.left)
          .recover { case t: Throwable =>
            logger.error(s"Projection process '${projection.id}' failed:", t)
            controlRef.get().tryError(t)
            ()
          }
          .mapAsync(1) { _ =>
            logger.trace(s"Closing resources for projection ${projection.id}")
            controlRef.get().tryComplete()
            closeResources()
          }
          .to(Sink.ignore)
      )
  }

  private def toConsumerRecord(
      projectionId: ProjectionId,
      predicate: Predicate[Event]) =
    Flow[GetTransactionsResponse].mapConcat { response =>
      response.transactions.flatMap { tx =>
        val transactionId = tx.transactionId
        val ledgerEffectiveTime =
          tx.effectiveAt.map(timestamp =>
            Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong))
        val workflowId = tx.workflowId
        val newProjectionOffset = Offset(tx.offset)
        val envelopes = tx.events.map { event =>
          Envelope[Event](
            event,
            Some(workflowId),
            ledgerEffectiveTime,
            Some(transactionId),
            Some(newProjectionOffset)
          )
        }.filter(predicate)
        if (envelopes.nonEmpty) {
          envelopes :+ TxBoundary[Event](projectionId, newProjectionOffset)
        } else {
          envelopes
        }
      }
    }

  private def getTransactionTrees(
      client: TransactionServiceClient,
      endOffset: Option[Offset],
      offset: Option[Offset],
      transactionFilter: TransactionFilter
  ): Source[GetTransactionTreesResponse, NotUsed] =
    client.getTransactionTrees(
      GetTransactionsRequest(
        begin = Some(Offset.protoOffset(offset)),
        end = endOffset.map(_.toProto),
        filter = Some(transactionFilter)
      )
    )

  private def getTransactions(
      client: TransactionServiceClient,
      endOffset: Option[Offset],
      offset: Option[Offset],
      transactionFilter: TransactionFilter
  ): Source[GetTransactionsResponse, NotUsed] =
    client.getTransactions(
      GetTransactionsRequest(
        begin = Some(Offset.protoOffset(offset)),
        end = endOffset.map(_.toProto),
        filter = Some(transactionFilter)
      )
    )

  private[projection] def activeContractConsumerRecordSource(
      source: Source[GetActiveContractsResponse, NotUsed],
      getTxFromProjection: (Option[Offset], TransactionFilter) => Source[GetTransactionsResponse, NotUsed],
      projection: Projection[Event],
      predicate: Predicate[Event]
  ): Source[ConsumerRecord[Event], NotUsed] = {
    source.flatMapConcat { response =>
      if (response.offset != "") {
        logger.trace(s"Received Offset ${response.offset} as last response of the ACS.")
        val newProjectionOffset = Offset(response.offset)
        val boundary = Source(List(TxBoundary[Event](projection.id, newProjectionOffset)))
        val nextEvents =
          getTxFromProjection(Some(newProjectionOffset), projection.transactionFilter).via(toConsumerRecord(
            projection.id,
            predicate))
        boundary.concat(nextEvents)
      } else {
        logger.trace(s"Received ACS response: $response")
        Source(response.activeContracts.map { event =>
          Envelope[Event](
            Event(Event.Event.Created(event)),
            None,
            None,
            None,
            projection.offset
          )
        }).filter(predicate)
      }
    }
  }

  private[projection] def activeContractSourceFromTxs(
      source: Source[GetActiveContractsResponse, NotUsed],
      getTxFromProjection: (Option[Offset], TransactionFilter) => Source[GetTransactionsResponse, NotUsed],
      projection: Projection[Event],
      predicate: Predicate[Event],
      batchSize: Int,
      interval: FiniteDuration
  ): Source[Batch[Event], NotUsed] =
    activeContractConsumerRecordSource(source, getTxFromProjection, projection, predicate)
      .via(Batcher[Event](batchSize, interval))

  private[projection] def exercisedEventSourceFromTrees(
      source: Source[GetTransactionTreesResponse, NotUsed],
      projection: Projection[ExercisedEvent],
      predicate: Predicate[ExercisedEvent],
      batchSize: Int,
      interval: FiniteDuration
  ): Source[Batch[ExercisedEvent], NotUsed] = {

    val tPredicate: Predicate[TreeEvent] = env => {
      env.event.kind match {
        case TreeEvent.Kind.Exercised(e) => predicate(env.map(_ => e))
        case _                           => false
      }
    }

    treeEventSourceFromTrees(source, projection.id, tPredicate, batchSize, interval).map {
      case batch: Batch[TreeEvent] =>
        Batch(
          batch.envelopes.flatMap { e =>
            e.traverseOption { case TreeEvent(TreeEvent.Kind.Exercised(exercisedEvent)) => exercisedEvent }
          },
          batch.boundary.map(b => b.coerce[ExercisedEvent]),
          batch.boundaryIndex
        )
    }
  }

  private[projection] def treeEventSourceFromTrees(
      txTreeSource: Source[GetTransactionTreesResponse, NotUsed],
      projectionId: ProjectionId,
      predicate: Predicate[TreeEvent],
      batchSize: Int,
      interval: FiniteDuration
  ): Source[Batch[TreeEvent], NotUsed] =
    treeEventConsumerRecordSource(txTreeSource, projectionId, predicate)
      .via(Batcher(batchSize, interval))

  private[projection] def treeEventConsumerRecordSource(
      txTreeSource: Source[GetTransactionTreesResponse, NotUsed],
      projectionId: ProjectionId,
      predicate: Predicate[TreeEvent]
  ): Source[ConsumerRecord[TreeEvent], NotUsed] = {
    txTreeSource
      .mapConcat { response =>
        response.transactions.flatMap { txTree =>
          val transactionId = txTree.transactionId
          val ledgerEffectiveTime =
            txTree.effectiveAt.map(timestamp =>
              Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong))
          val workflowId = txTree.workflowId
          val newProjectionOffset = Offset(txTree.offset)
          val envelopes = txTree.rootEventIds.flatMap { eventId =>
            val event = txTree.eventsById(eventId)
            val envelope =
              Envelope[TreeEvent](
                event,
                Some(workflowId),
                ledgerEffectiveTime,
                Some(transactionId),
                Some(newProjectionOffset)
              )
            if (predicate(envelope)) Some(envelope) else None
          }
          if (envelopes.nonEmpty) {
            envelopes :+ TxBoundary[TreeEvent](projectionId, newProjectionOffset)
          } else {
            envelopes
          }
        }
      }
  }
}

trait Control {

  /**
   * Cancels the projection.
   */
  def cancel(): Future[Done]

  /**
   * The returned `Future` will be successfully completed with the `Throwable` that caused the `Projection` to fail.
   *
   * If the `Projection` is successful and has completed, the returned `Future` is failed with a
   * `NoSuchElementException`. The returned `Future` will not complete if the `Projection` runs continuously and does
   * not fail.
   */
  def failed: Future[Throwable]

  /**
   * The returned `Future` will be successfully completed with `Done` when the projection has completed.
   *
   * The returned `Future` will not complete if the `Projection` runs continuously and does not fail.
   */
  def completed: Future[Done] = resourcesClosed

  /**
   * Returns if all resources used by the projection have been closed.
   */
  def resourcesClosed: Future[Done]

  def asJava: com.daml.projection.javadsl.Control

  private[projection] def tryError(t: Throwable): Boolean
  private[projection] def tryComplete(): Boolean
}

object GrpcControl {
  def closeClients(
      clients: Seq[akka.grpc.scaladsl.AkkaGrpcClient]
  )(implicit ec: ExecutionContext): Future[Done] =
    Future
      .sequence(clients.map { c =>
        c.close().flatMap(_ => c.closed)
      })
      .map(_ => Done)
}

final case class GrpcControl(
    clients: Seq[akka.grpc.scaladsl.AkkaGrpcClient],
    killSwitch: KillSwitch
)(implicit ec: ExecutionContext)
    extends Control {
  val promise: Promise[Throwable] = Promise[Throwable]()
  def cancel(): Future[Done] = {
    killSwitch.shutdown()
    GrpcControl.closeClients(clients)
  }
  def tryError(t: Throwable) = promise.trySuccess(t)
  def tryComplete() = promise.tryFailure(new NoSuchElementException())
  def failed: Future[Throwable] = promise.future
  def resourcesClosed: Future[Done] =
    Future
      .sequence(clients.map(_.closed))
      .map(_ => Done)
  def asJava = new com.daml.projection.javadsl.GrpcControl(this)
}
