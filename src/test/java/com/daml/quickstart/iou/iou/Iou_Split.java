// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou.iou;

import com.daml.ledger.javaapi.data.Numeric;
import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.codegen.DamlRecord;
import com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders;
import com.daml.ledger.javaapi.data.codegen.ValueDecoder;
import java.lang.Deprecated;
import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Iou_Split extends DamlRecord<Iou_Split> {
  public static final String _packageId = "407382e4ffc06fed9636591a1196d8ce97961b51b1398fb9da5db679ec6e45fc";

  public final BigDecimal splitAmount;

  public Iou_Split(BigDecimal splitAmount) {
    this.splitAmount = splitAmount;
  }

  /**
   * @deprecated since Daml 2.5.0; use {@code valueDecoder} instead */
  @Deprecated
  public static Iou_Split fromValue(Value value$) throws IllegalArgumentException {
    return valueDecoder().decode(value$);
  }

  public static ValueDecoder<Iou_Split> valueDecoder() throws IllegalArgumentException {
    return value$ -> {
      Value recordValue$ = value$;
      List<com.daml.ledger.javaapi.data.DamlRecord.Field> fields$ = PrimitiveValueDecoders.recordCheck(1,
          recordValue$);
      BigDecimal splitAmount = PrimitiveValueDecoders.fromNumeric.decode(fields$.get(0).getValue());
      return new Iou_Split(splitAmount);
    } ;
  }

  public com.daml.ledger.javaapi.data.DamlRecord toValue() {
    ArrayList<com.daml.ledger.javaapi.data.DamlRecord.Field> fields = new ArrayList<com.daml.ledger.javaapi.data.DamlRecord.Field>(1);
    fields.add(new com.daml.ledger.javaapi.data.DamlRecord.Field("splitAmount", new Numeric(this.splitAmount)));
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
    if (!(object instanceof Iou_Split)) {
      return false;
    }
    Iou_Split other = (Iou_Split) object;
    return Objects.equals(this.splitAmount, other.splitAmount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.splitAmount);
  }

  @Override
  public String toString() {
    return String.format("com.daml.quickstart.iou.iou.Iou_Split(%s)", this.splitAmount);
  }
}
