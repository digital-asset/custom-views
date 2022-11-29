// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou.iou;

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

public class Iou_Merge extends DamlRecord<Iou_Merge> {
  public static final String _packageId = "407382e4ffc06fed9636591a1196d8ce97961b51b1398fb9da5db679ec6e45fc";

  public final Iou.ContractId otherCid;

  public Iou_Merge(Iou.ContractId otherCid) {
    this.otherCid = otherCid;
  }

  /**
   * @deprecated since Daml 2.5.0; use {@code valueDecoder} instead */
  @Deprecated
  public static Iou_Merge fromValue(Value value$) throws IllegalArgumentException {
    return valueDecoder().decode(value$);
  }

  public static ValueDecoder<Iou_Merge> valueDecoder() throws IllegalArgumentException {
    return value$ -> {
      Value recordValue$ = value$;
      List<com.daml.ledger.javaapi.data.DamlRecord.Field> fields$ = PrimitiveValueDecoders.recordCheck(1,
          recordValue$);
      Iou.ContractId otherCid =
          new Iou.ContractId(fields$.get(0).getValue().asContractId().orElseThrow(() -> new IllegalArgumentException("Expected otherCid to be of type com.daml.ledger.javaapi.data.ContractId")).getValue());
      return new Iou_Merge(otherCid);
    } ;
  }

  public com.daml.ledger.javaapi.data.DamlRecord toValue() {
    ArrayList<com.daml.ledger.javaapi.data.DamlRecord.Field> fields = new ArrayList<com.daml.ledger.javaapi.data.DamlRecord.Field>(1);
    fields.add(new com.daml.ledger.javaapi.data.DamlRecord.Field("otherCid", this.otherCid.toValue()));
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
    if (!(object instanceof Iou_Merge)) {
      return false;
    }
    Iou_Merge other = (Iou_Merge) object;
    return Objects.equals(this.otherCid, other.otherCid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.otherCid);
  }

  @Override
  public String toString() {
    return String.format("com.daml.quickstart.iou.iou.Iou_Merge(%s)", this.otherCid);
  }
}
