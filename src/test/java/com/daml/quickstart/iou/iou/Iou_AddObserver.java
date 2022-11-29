// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou.iou;

import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.codegen.DamlRecord;
import com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders;
import com.daml.ledger.javaapi.data.codegen.ValueDecoder;
import java.lang.Deprecated;
import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Iou_AddObserver extends DamlRecord<Iou_AddObserver> {
  public static final String _packageId = "407382e4ffc06fed9636591a1196d8ce97961b51b1398fb9da5db679ec6e45fc";

  public final String newObserver;

  public Iou_AddObserver(String newObserver) {
    this.newObserver = newObserver;
  }

  /**
   * @deprecated since Daml 2.5.0; use {@code valueDecoder} instead */
  @Deprecated
  public static Iou_AddObserver fromValue(Value value$) throws IllegalArgumentException {
    return valueDecoder().decode(value$);
  }

  public static ValueDecoder<Iou_AddObserver> valueDecoder() throws IllegalArgumentException {
    return value$ -> {
      Value recordValue$ = value$;
      List<com.daml.ledger.javaapi.data.DamlRecord.Field> fields$ = PrimitiveValueDecoders.recordCheck(1,
          recordValue$);
      String newObserver = PrimitiveValueDecoders.fromParty.decode(fields$.get(0).getValue());
      return new Iou_AddObserver(newObserver);
    } ;
  }

  public com.daml.ledger.javaapi.data.DamlRecord toValue() {
    ArrayList<com.daml.ledger.javaapi.data.DamlRecord.Field> fields = new ArrayList<com.daml.ledger.javaapi.data.DamlRecord.Field>(1);
    fields.add(new com.daml.ledger.javaapi.data.DamlRecord.Field("newObserver", new Party(this.newObserver)));
    return new com.daml.ledger.javaapi.data.DamlRecord(fields);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (!(object instanceof Iou_AddObserver)) {
      return false;
    }
    Iou_AddObserver other = (Iou_AddObserver) object;
    return Objects.equals(this.newObserver, other.newObserver);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.newObserver);
  }

  @Override
  public String toString() {
    return String.format("com.daml.quickstart.iou.iou.Iou_AddObserver(%s)", this.newObserver);
  }
}
