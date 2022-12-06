// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projectiontest;

import akka.actor.ActorSystem;
import akka.grpc.GrpcClientSettings;
import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.Event;
import com.daml.projection.*;
import com.daml.projection.javadsl.*;
import com.daml.quickstart.iou.iou.Iou;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;
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
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(url);
      config.setUsername(user);
      config.setPassword(password);

      HikariDataSource ds = new HikariDataSource(config);
      var system = ActorSystem.create("my-projection-app");
      var projector = JdbcProjector.create(ds, system);

      var projectionTable = new ProjectionTable("ious");

      // A projection is automatically updated and resumed, if it is found in the projection table.
      // if bad data is encountered, the projection does not continue.
      var events =
          Projection.<Event>create(new ProjectionId("iou-projection-for-party"), ProjectionFilter.parties(Set.of(partyId)), projectionTable)
              .withBatchSize(batchSize);
      Project<Event, JdbcAction> f =
          envelope -> {
            var event = envelope.getEvent();
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
            } else {
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

      var grpcClientSettings = GrpcClientSettings.connectToServiceAt("localhost", 6865, system);
      var source = BatchSource.events(grpcClientSettings);
      var control = projector.project(source, events, f);
      control.cancel();

      var contracts = Projection.<Iou.Contract>create(new ProjectionId("id"), ProjectionFilter.parties(Set.of(partyId)), projectionTable);
      Project<Iou.Contract, JdbcAction> fc =
          envelope -> {
            var iou = envelope.getEvent();
            var action = ExecuteUpdate.create(
                    "insert into "
                        + envelope.getProjectionTable().getName()
                        + "(contract_id, event_id, amount, currency) "
                        + "values (?, ?, ?, ?)")
                .bind(1, iou.id.contractId)
                .bind(2, null)
                .bind(3, iou.data.amount)
                .bind(4, iou.data.currency);
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
          .bind("contract_id", iou -> iou.id.contractId)
          .bind("event_id", iou -> null)
          .bind("amount", iou -> iou.data.amount)
          .bind("currency", iou -> iou.data.currency);

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
