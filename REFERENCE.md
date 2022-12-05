# Projection

## Overview
Three kinds of protobuf events can be projected into SQL tables:
- `Event` (`CreatedEvent` or `ArchivedEvent`)
- `ExercisedEvent`
- `TreeEvent`

The following `Projection`s are supported:

- A `Projection[Event]` for projecting `Event`s.
- A `Projection[ExercisedEvent]` for projecting `ExercisedEvent`s.
- A `Projection[TreeEvent]` for projecting `TreeEvent`s.

A Projection: 
- has a `ProjectionId` that must uniquely identify the projection process.
- has an `Offset` which is used as a starting point to read from the ledger. 
- specifies an SQL table to project to. (for informational purposes and easy access to the table name from the projection)
- defines a `TransactionFilter`, which is used to select events from the ledger 
(For instance, visible to a set of parties, for a set of template ID's). 

A newly created projection by default has no offset, which means a projection will start from the beginning.
A projection is updated when it successfully commits transactions into the SQL database according to transactions that were committed on the ledger.
A projection will be resumed from its stored offset automatically, if it can be found by its `ProjectionId`. 

A common workflow for setting up a projection follows:

- Create a table in your SQL database. (Currently the library does not provide features for this)
- Setup a `ProjectionSettings` with details how to connect to the ledger, amongst other settings, create a `Projector`.
- Create a `Projection[E]`. If the projection with ID already exists in the database, it will continue where it left off. 
- Create a `Project[E, A]` function that transforms an event into (0 to N) database actions.
- invoke `project` on the `Projector`, passing in the projection, and the projection function. 
This starts the projection process, and returns a `Control` to cancel it.
- cancel the projection by invoking `control.cancel` on shutdown of your application. 

## Scala API

[doobie](https://tpolecat.github.io/doobie/) is currently used to execute the database actions, projecting ledger event data into SQL tables.
The `Doobie` object provides:

- A `Doobie.Action` type alias, which captures the database action that should be executed.
- A `Doobie.Projector` object, to execute the database actions.

The `Projection` companion object provides `project` methods that take a `Projection[E]`, and a `Project[E, A]` function (and implicitly a `Projector`).
The `Project[E, A]` function must transform an event into database `Action`s (or an empty list if no action should occur).
A `Projector` executes the database actions and automatically advances the projection according to ledger transactions.
The `project` method can also take a `BatchOperation` (`Doobie.Copy` or `Doobie.UpdateMany`), which can be used to execute batch / bulk database operations. 

### Envelope

The events from the ledger are wrapped in an `Envelope[E]`. the envelope has the following fields: 

- event. (the wrapped value)
- offset: the offset of the event.
- table: the (main) `ProjectionTable` that is projected to.
- workflowId (optional)
- ledgerEffectiveTime (optional)
- transactionId (optional)

### Project[E, A]

The `Project[E,A]` function projects an event `E` into an `Iterable[A]` where `A` is a database action. 

### Examples
The below example shows a projection that inserts iou data from every created event, and deletes on archive event, over time providing a current set of iou data.

```scala
import Doobie._
implicit val projector = Projector()

val partyId = "alice"
val projectionId = ProjectionId("for-alice")
val projectionTable = "ious"
val projection = Projection[Event](projectionId, Set(partyId), projectionTable)

// Define how the created event should be projected into actions with a `Project` function.
val insert: Project[CreatedEvent] = { envelope =>
  import envelope._
  val witnessParties = event.witnessParties.mkString(",")
  val iou = toIou(event)

  List(fr"""insert into ${table.const} 
    (contract_id, event_id, witness_parties, amount, currency)
    values (${event.contractId}, ${event.eventId}, $witnessParties, ${iou.data.amount}, ${iou.data.currency})""".update.run)
}

// Define how the archived event should be projected into actions with a `Project` function.
val delete: Project[ArchivedEvent] = { envelope =>
  import envelope._
  List(fr"delete from ${table.const} where contract_id = ${event.contractId}".update.run)
}

// continuously project the events
val source = BatchSource.events(clientSettings)
val ctrl = Projection.project(source, projection, insert, delete)
// possibly cancel the projection
ctrl.cancel()
```

The below example shows a projection that inserts exercised event data.

```scala
import com.daml.projection.GrpcBatchSource
implicit val projector = Projector()
val projectionId = ProjectionId("exercised-events-for-alice")
val exercisedEvents = Projection[ExercisedEvent](projectionId, List("alice"), ProjectionTable("contracts"))
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
ctrl.cancel()
```

An example of using a batch operator, `Doobie.UpdateMany` (which uses doobie's `updateMany` internally).

```scala
import Doobie._
implicit val projector = Projector()

val partyId = "alice"
val projectionId = ProjectionId("for-alice")
val projectionTable = "ious"
val projection = Projection[Event](projectionId, Set(partyId), projectionTable)

case class CreatedRow(contractId: String, eventId: String, amount: BigDecimal, currency: String)
val sql =
  s"""insert into ${projection.table.name}(contract_id, event_id,  amount, currency) " +
  s"values (?, ?, ?, ?)"""

val source = BatchSource.events(clientSettings)
val ctrl = projection.project(
  source,
  projection,
  Doobie.UpdateMany[CreatedRow](sql)) { envelope =>
  import envelope._

  event match {
    case Event(Created(ce)) =>
      val iou = toIou(ce)
      List(CreatedRow(ce.contractId, ce.eventId, iou.data.amount, iou.data.currency))
    case _ => List()
  }
}
ctrl.cancel

// using codegen
def toIou(event: CreatedEvent) =
  Iou.COMPANION.fromCreatedEvent(JCreatedEvent.fromProto(CreatedEvent.toJavaProto(event)))

```

See the [FacadeSpec](src/test/scala/com/daml/projectiontest/FacadeSpec.scala) for more usages.

### Low level Akka stream API  
NOTE: We will unlikely provide these as an API to the end-user, or move these to INTERNAL API, because of licensing changes.

[Akka stream](https://doc.akka.io/docs/akka/current/stream/index.html) is used to define Akka stream sources, flows and sinks to read ledger events, and transform these into database actions.

A `Consumer` object provides sources for consuming ledger events.
A common workflow for setting up a projection follows:

- Create a `Projection[E]`. 
- Get a `Source[Batch[E], Control]` from the `Consumer`  using the `projection`.
- Create an `Action` flow that turns specific events into database actions. 
- Compose an akka stream graph from a source, an `Action` flow and a `Projector` flow.
- Run the graph. Once materialized the graph will run continuously until it is cancelled.
- The graph materializes into a `Control`, which has a `cancel()` method to cancel the stream.

The example below shows how the akka stream API can be used:

```scala
// Create a Projection
val partyId = "alice"
val projectionId = ProjectionId("for-alice")
val projectionTable = "ious"
val projection = Projection[Event](projectionId, Set(partyId), projectionTable)

// Get source of events
val source: Source[Batch[Event], Control] =
  Consumer.eventSource(clientSettings, projection)

// transform create events into inserts
val createAction: Project[CreatedEvent, Action] = { envelope =>
  import envelope._
  val witnessParties = event.witnessParties.mkString(",")
  List(fr"""insert into ${table.const} (contract_id, event_id, witness_parties) values (${event.contractId}, ${event.eventId}, $witnessParties)""".update.run)
}

// transform archive events into deletes
val archiveAction: Project[ArchivedEvent, Action] = { envelope =>
  import envelope._
  List(fr"delete from ${table.const} where contract_id = ${event.contractId}".update.run)
}

// compose akka stream graph and run it, returning the `Control` to cancel the stream if necessary.
val control = source
  .via(Projection.flow(projection, createAction, archiveAction))
  .via(Projector.flow(xa))
  .toMat(Sink.ignore)(Keep.left).run()

// cancel the stream, if necessary
control.cancel()
``` 

See the [ProjectionSpec](src/test/scala/com/daml/projection/ProjectionSpec.scala) for more usages.

#### Consumer
The Consumer provides:

- An `eventSource` to read created and archived events.
- An `exercisedEventSource` to read exercised events.
- A `treeEventSource` to read tree events.

All sources return a `Source[Batch[E], Control]` where `E` is the specific event.
A `Batch[E]` contains envelopes.
An `Envelope[E]` contains the ledger event `E` along with some additional transaction information, which should provide enough information to translate into a database action.

Events are added to a `Batch` inside `Envelope`s. 
The `Batch` optionally contains a `TxBoundary` (transaction boundary), which indicates that a ledger transaction has been concluded. 
This boundary is optional to make it possible to process larger-than-memory transactions. 
The max batch size is configurable in `ProjectionSettings`.
A batch can be smaller than max size when a ledger transaction boundary is encountered.
A batch is produced by the source on every completion of a Daml transaction, or when the `batchSize` has been reached while adding to the batch.

The `TxBoundary` is internally used to:

- Automatically update the projection after database actions are executed for the same Daml transaction.
- To commit the database actions that have been executed up to the point of the boundary, so that _view transactionality_ is preserved (changes to the projection table are visible as you would expect daml transactions to be visible). 
- Everything between two tx boundaries is treated as one SQL transaction.

## Dispatcher configuration for blocking operation

A default dedicated dispatcher for blocking operations (e.g. db operation) is configured in reference.conf:

```
projection {
  blocking-io-dispatcher {
    type = Dispatcher
    executor = "thread-pool-executor"
    thread-pool-executor {
      fixed-pool-size = 16
    }
    throughput = 1
  }
}
```

You can override this dispatcher through the application.conf in your application.

## Ledger API Authorization

The client must provide an access token when authorization is required by the Ledger.
Detail of Ledger Authorization please refer to [Ledger Authorization documentation](https://docs.daml.com/app-dev/authorization.html)

### Provide access token to custom-view library

Applications can provide an access token when setting up the client. 

```scala
val clientSettings = GrpcClientSettings
  .connectToServiceAt(host, port.value)
  .withCallCredentials(new LedgerCallCredentials(accessToken))
val source = BatchSource.events(clientSettings)
val control = Projection.project(source, exercisedEvents)(f)
```

### Provide a newly retrieved access token when the existing one expired

When the access token is expired, application can retrieve a new access token with the stored refresh token.
Detail of refresh access token please refer to [Ledger auth-middleware documentation](https://docs.daml.com/tools/auth-middleware/index.html#refresh-access-token)
Application can cancel the running projection and re-create a new one with the new access token.

```scala
control.cancel().map(_ => {
  val sourceWithNewToken = BatchSource.events(
    clientSettings.withCallCredentials(new LedgerCallCredentials(newAccessToken))
  )
  val newControl = Projection.project(sourceWithNewToken, exercisedEvents)(f)
  newControl
})
```
