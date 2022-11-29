// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou.ioutrade;

import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.codegen.DamlRecord;
import com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders;
import com.daml.ledger.javaapi.data.codegen.ValueDecoder;
import com.daml.quickstart.iou.iou.Iou;
import java.lang.Deprecated;
import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IouTrade_Accept extends DamlRecord<IouTrade_Accept> {
  public static final String _packageId = "407382e4ffc06fed9636591a1196d8ce97961b51b1398fb9da5db679ec6e45fc";

  public final Iou.ContractId quoteIouCid;

  public IouTrade_Accept(Iou.ContractId quoteIouCid) {
    this.quoteIouCid = quoteIouCid;
  }

  /**
   * @deprecated since Daml 2.5.0; use {@code valueDecoder} instead */
  @Deprecated
  public static IouTrade_Accept fromValue(Value value$) throws IllegalArgumentException {
    return valueDecoder().decode(value$);
  }

  public static ValueDecoder<IouTrade_Accept> valueDecoder() throws IllegalArgumentException {
    return value$ -> {
      Value recordValue$ = value$;
      List<com.daml.ledger.javaapi.data.DamlRecord.Field> fields$ = PrimitiveValueDecoders.recordCheck(1,
          recordValue$);
      Iou.ContractId quoteIouCid =
          new Iou.ContractId(fields$.get(0).getValue().asContractId().orElseThrow(() -> new IllegalArgumentException("Expected quoteIouCid to be of type com.daml.ledger.javaapi.data.ContractId")).getValue());
      return new IouTrade_Accept(quoteIouCid);
    } ;
  }

  public com.daml.ledger.javaapi.data.DamlRecord toValue() {
    ArrayList<com.daml.ledger.javaapi.data.DamlRecord.Field> fields = new ArrayList<com.daml.ledger.javaapi.data.DamlRecord.Field>(1);
    fields.add(new com.daml.ledger.javaapi.data.DamlRecord.Field("quoteIouCid", this.quoteIouCid.toValue()));
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
    if (!(object instanceof IouTrade_Accept)) {
      return false;
    }
    IouTrade_Accept other = (IouTrade_Accept) object;
    return Objects.equals(this.quoteIouCid, other.quoteIouCid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.quoteIouCid);
  }

  @Override
  public String toString() {
    return String.format("com.daml.quickstart.iou.ioutrade.IouTrade_Accept(%s)", this.quoteIouCid);
  }
}
