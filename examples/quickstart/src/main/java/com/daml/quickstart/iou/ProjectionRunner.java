package com.daml.quickstart.iou;

import akka.actor.ActorSystem;
import akka.grpc.GrpcClientSettings;
import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.Event;
import com.daml.projection.Projection;
import com.daml.projection.ProjectionId;
import com.daml.projection.ProjectionFilter;
import com.daml.projection.ProjectionTable;
import com.daml.projection.javadsl.*;
import com.daml.quickstart.model.iou.Iou;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ProjectionRunner {
  private static final Logger logger = LoggerFactory.getLogger(ProjectionRunner.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    if(args.length < 1)
      throw new IllegalArgumentException("An argument for party of Alice is expected.");

    var aliceParty = args[0];

    // Setup db params
    String url = "jdbc:postgresql://localhost/ious";
    String user = "postgres";
    String password = "postgres";

    ActorSystem system = ActorSystem.create("iou-projection");
    ConnectionSupplier connectionSupplier = () -> java.sql.DriverManager.getConnection(url, user, password);
    Projector<JdbcAction> projector = JdbcProjector.create(connectionSupplier, system);
    ProjectionTable projectionTable = new ProjectionTable("events");

    Projection<Event> events =
        Projection.<Event>create(new ProjectionId("active-iou-contrants-for-alice"), ProjectionFilter.parties(Set.of(aliceParty)), projectionTable);

    Project<Event, JdbcAction> f = envelope -> {
      Event event = envelope.getEvent();
      logger.info("projecting event " + event.getEventId());
      if (event instanceof CreatedEvent) {
        Iou.Contract iou = Iou.Contract.fromCreatedEvent((CreatedEvent) event);
        var action =
            ExecuteUpdate.create(
                    "insert into "
                        + envelope.getProjectionTable().getName()
                        + "(contract_id, event_id, amount, currency) "
                        + "values (?, ?, ?, ?)")
                .bind(1, event.getContractId())
                .bind(2, event.getEventId())
                .bind(3, iou.data.amount)
                .bind(4, iou.data.currency);
        return List.of(action);
      }
      else {
        var action =
            ExecuteUpdate.create(
                    "delete from " +
                        envelope.getProjectionTable().getName() +
                        " where contract_id = ?"
                )
                .bind(1, event.getContractId());
        return List.of(action);
      }
    };

    GrpcClientSettings grpcClientSettings = GrpcClientSettings
        .connectToServiceAt("localhost", 6865, system)
        .withTls(false);
    var source = BatchSource.events(grpcClientSettings);

    logger.info("Starting projection");

    Control control = projector.project(source, events, f);

    control.failed().whenComplete((throwable, ignored) -> {
      if (throwable instanceof NoSuchElementException)
        logger.info("Projection finished.");
      else
        logger.error("Failed to run Projection.", throwable);
      control.resourcesClosed().thenRun(() -> system.terminate());
    });
  }
}
