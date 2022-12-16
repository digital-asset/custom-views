// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projectiontest;

import akka.actor.ActorSystem;
import akka.grpc.GrpcClientSettings;
import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.Event;
import com.daml.projection.*;
import com.daml.projection.javadsl.*;
import com.daml.quickstart.model.iou.Iou;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/*
 * An example of how the Java API can be used.
 */
public class JavaApiExample {
  public static void main(String[] args) {
    // max nr of events in memory while streaming from the ledger
    var batchSize = 10000;
    var url = "jdbc:postgresql://localhost/db";
    var user = "postgres";
    var password = "postgres";
    var partyId = "alice";

    try {
      var config = new HikariConfig();
      config.setJdbcUrl(url);
      config.setUsername(user);
      config.setPassword(password);

      var ds = new HikariDataSource(config);
      var system = ActorSystem.create("my-projection-app");
      var projector = JdbcProjector.create(ds, system);

      var projectionTable = new ProjectionTable("ious");

      // A projection is automatically updated and resumed, if it is found in the projection table.
      // if bad data is encountered, the projection does not continue.
      var events =
          Projection.<Event>create(new ProjectionId("iou-projection-for-party"), ProjectionFilter.parties(Set.of(partyId)))
              .withBatchSize(batchSize);
      Project<Event, JdbcAction> f =
          envelope -> {
            var event = envelope.getEvent();
            if (event instanceof CreatedEvent) {
              Iou.Contract iou = Iou.Contract.fromCreatedEvent((CreatedEvent) event);
              // To verify that a subclass can use the Bind of the superclass.
              var br = new MyBigDecimal(iou.data.amount.toString());
              // To verify that binding optional compiles.
              Optional<String> optionalEventId = Optional.of(event.getEventId());

              var action =
                  ExecuteUpdate.create(
                          "insert into "
                              + projectionTable.getName()
                              + "(contract_id, event_id, amount, currency) "
                              + "values (?, ?, ?, ?)")
                      .bind(1, event.getContractId(), Bind.String())
                      .bind(2, optionalEventId, Bind.Optional(Bind.String()))
                      .bind(3, Optional.of(br), Bind.Optional(Bind.BigDecimal()))
                      .bind(4, iou.data.currency, Bind.String());
              return List.of(action);
            } else {
              var action =
                  ExecuteUpdate.create(
                          "delete from " +
                              projectionTable.getName() +
                              " where contract_id = ?"
                      )
                      .bind(1, event.getContractId(), Bind.String());
              return List.of(action);
            }
          };

      var grpcClientSettings = GrpcClientSettings.connectToServiceAt("localhost", 6865, system);
      var source = BatchSource.events(grpcClientSettings);
      var control = projector.project(source, events, f);
      control.cancel();

      var contracts = Projection.<Iou.Contract>create(new ProjectionId("id"), ProjectionFilter.parties(Set.of(partyId)));

      Project<Iou.Contract, JdbcAction> fc =
          envelope -> {
            var iou = envelope.getEvent();
            var eventId = Optional.<String>empty();
            // To test for subclass
            var mybd = new MyBigDecimal(iou.data.amount.toString());
            var action = ExecuteUpdate.create(
                    "insert into "
                        + projectionTable.getName()
                        + "(contract_id, event_id, amount, currency) "
                        + "values (?, ?, ?, ?)")
                .bind(1, iou.id.contractId, Bind.String())
                .bind(2, eventId, Bind.Optional(Bind.String()))
                .bind(3, mybd, Bind.BigDecimal())
                .bind(4, iou.data.currency, Bind.String());
            return List.of(action);
          };

      var batchSource = BatchSource.create(grpcClientSettings,
          e -> {
            return Iou.Contract.fromCreatedEvent(e);
          });
      var ccontrol =
          projector.project(
              batchSource,
              contracts,
              fc
          );
      ccontrol.cancel();

      Project<Iou.Contract, Iou.Contract> mkRow =
          envelope -> {
            return List.of(envelope.getEvent());
          };

      Binder<Iou.Contract> binder = Sql.<Iou.Contract>binder("insert into "
              + projectionTable.getName()
              + "(contract_id, event_id, amount, currency) "
              + "values (:contract_id, :event_id, :amount, :currency)")
          .bind("contract_id", iou -> iou.id.contractId, Bind.String())
          .bind("event_id", iou -> null, Bind.String())
          .bind("amount", iou -> iou.data.amount, Bind.BigDecimal())
          .bind("currency", iou -> iou.data.currency, Bind.String());

      BatchRows<Iou.Contract, JdbcAction> batchRows =
          UpdateMany.create(binder);

      var bcontrol =
          projector.projectRows(
              batchSource,
              contracts,
              batchRows,
              mkRow
          );
      bcontrol.cancel();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
