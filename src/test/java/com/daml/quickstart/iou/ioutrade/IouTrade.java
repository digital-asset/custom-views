// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou.ioutrade;

import com.daml.ledger.javaapi.data.ContractFilter;
import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;
import com.daml.ledger.javaapi.data.CreateCommand;
import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.ExerciseCommand;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.Numeric;
import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.Template;
import com.daml.ledger.javaapi.data.Text;
import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.codegen.Choice;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion;
import com.daml.ledger.javaapi.data.codegen.Created;
import com.daml.ledger.javaapi.data.codegen.Exercised;
import com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders;
import com.daml.ledger.javaapi.data.codegen.Update;
import com.daml.ledger.javaapi.data.codegen.ValueDecoder;
import com.daml.quickstart.iou.da.internal.template.Archive;
import com.daml.quickstart.iou.da.types.Tuple2;
import com.daml.quickstart.iou.iou.Iou;
import java.lang.Deprecated;
import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class IouTrade extends Template {
  public static final Identifier TEMPLATE_ID = new Identifier("407382e4ffc06fed9636591a1196d8ce97961b51b1398fb9da5db679ec6e45fc", "IouTrade", "IouTrade");

  public static final Choice<IouTrade, IouTrade_Accept, Tuple2<Iou.ContractId, Iou.ContractId>> CHOICE_IouTrade_Accept = 
      Choice.create("IouTrade_Accept", value$ -> value$.toValue(), value$ ->
        IouTrade_Accept.valueDecoder().decode(value$), value$ ->
        Tuple2.<com.daml.quickstart.iou.iou.Iou.ContractId,
        com.daml.quickstart.iou.iou.Iou.ContractId>valueDecoder(v$0 ->
          new Iou.ContractId(v$0.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()),
        v$1 ->
          new Iou.ContractId(v$1.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()))
        .decode(value$));

  public static final Choice<IouTrade, Archive, Unit> CHOICE_Archive = 
      Choice.create("Archive", value$ -> value$.toValue(), value$ -> Archive.valueDecoder()
        .decode(value$), value$ -> PrimitiveValueDecoders.fromUnit.decode(value$));

  public static final Choice<IouTrade, TradeProposal_Reject, Unit> CHOICE_TradeProposal_Reject = 
      Choice.create("TradeProposal_Reject", value$ -> value$.toValue(), value$ ->
        TradeProposal_Reject.valueDecoder().decode(value$), value$ ->
        PrimitiveValueDecoders.fromUnit.decode(value$));

  public static final ContractCompanion.WithoutKey<Contract, ContractId, IouTrade> COMPANION = 
      new ContractCompanion.WithoutKey<>("com.daml.quickstart.iou.ioutrade.IouTrade", TEMPLATE_ID,
        ContractId::new, v -> IouTrade.templateValueDecoder().decode(v), Contract::new,
        List.of(CHOICE_IouTrade_Accept, CHOICE_Archive, CHOICE_TradeProposal_Reject));

  public final String buyer;

  public final String seller;

  public final Iou.ContractId baseIouCid;

  public final String baseIssuer;

  public final String baseCurrency;

  public final BigDecimal baseAmount;

  public final String quoteIssuer;

  public final String quoteCurrency;

  public final BigDecimal quoteAmount;

  public IouTrade(String buyer, String seller, Iou.ContractId baseIouCid, String baseIssuer,
      String baseCurrency, BigDecimal baseAmount, String quoteIssuer, String quoteCurrency,
      BigDecimal quoteAmount) {
    this.buyer = buyer;
    this.seller = seller;
    this.baseIouCid = baseIouCid;
    this.baseIssuer = baseIssuer;
    this.baseCurrency = baseCurrency;
    this.baseAmount = baseAmount;
    this.quoteIssuer = quoteIssuer;
    this.quoteCurrency = quoteCurrency;
    this.quoteAmount = quoteAmount;
  }

  @Override
  public Update<Created<com.daml.ledger.javaapi.data.codegen.ContractId<IouTrade>>> create() {
    return new Update.CreateUpdate<com.daml.ledger.javaapi.data.codegen.ContractId<IouTrade>, Created<com.daml.ledger.javaapi.data.codegen.ContractId<IouTrade>>>(new CreateCommand(IouTrade.TEMPLATE_ID, this.toValue()), x -> x, ContractId::new);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIouTrade_Accept} instead */
  @Deprecated
  public Update<Exercised<Tuple2<Iou.ContractId, Iou.ContractId>>> createAndExerciseIouTrade_Accept(
      IouTrade_Accept arg) {
    return createAnd().exerciseIouTrade_Accept(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIouTrade_Accept} instead */
  @Deprecated
  public Update<Exercised<Tuple2<Iou.ContractId, Iou.ContractId>>> createAndExerciseIouTrade_Accept(
      Iou.ContractId quoteIouCid) {
    return createAndExerciseIouTrade_Accept(new IouTrade_Accept(quoteIouCid));
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseArchive} instead */
  @Deprecated
  public Update<Exercised<Unit>> createAndExerciseArchive(Archive arg) {
    return createAnd().exerciseArchive(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseTradeProposal_Reject} instead */
  @Deprecated
  public Update<Exercised<Unit>> createAndExerciseTradeProposal_Reject(TradeProposal_Reject arg) {
    return createAnd().exerciseTradeProposal_Reject(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseTradeProposal_Reject} instead */
  @Deprecated
  public Update<Exercised<Unit>> createAndExerciseTradeProposal_Reject() {
    return createAndExerciseTradeProposal_Reject(new TradeProposal_Reject());
  }

  public static Update<Created<com.daml.ledger.javaapi.data.codegen.ContractId<IouTrade>>> create(
      String buyer, String seller, Iou.ContractId baseIouCid, String baseIssuer,
      String baseCurrency, BigDecimal baseAmount, String quoteIssuer, String quoteCurrency,
      BigDecimal quoteAmount) {
    return new IouTrade(buyer, seller, baseIouCid, baseIssuer, baseCurrency, baseAmount,
        quoteIssuer, quoteCurrency, quoteAmount).create();
  }

  @Override
  public CreateAnd createAnd() {
    return new CreateAnd(this);
  }

  @Override
  protected ContractCompanion.WithoutKey<Contract, ContractId, IouTrade> getCompanion() {
    return COMPANION;
  }

  /**
   * @deprecated since Daml 2.5.0; use {@code valueDecoder} instead */
  @Deprecated
  public static IouTrade fromValue(Value value$) throws IllegalArgumentException {
    return valueDecoder().decode(value$);
  }

  public static ValueDecoder<IouTrade> valueDecoder() throws IllegalArgumentException {
    return ContractCompanion.valueDecoder(COMPANION);
  }

  public DamlRecord toValue() {
    ArrayList<DamlRecord.Field> fields = new ArrayList<DamlRecord.Field>(9);
    fields.add(new DamlRecord.Field("buyer", new Party(this.buyer)));
    fields.add(new DamlRecord.Field("seller", new Party(this.seller)));
    fields.add(new DamlRecord.Field("baseIouCid", this.baseIouCid.toValue()));
    fields.add(new DamlRecord.Field("baseIssuer", new Party(this.baseIssuer)));
    fields.add(new DamlRecord.Field("baseCurrency", new Text(this.baseCurrency)));
    fields.add(new DamlRecord.Field("baseAmount", new Numeric(this.baseAmount)));
    fields.add(new DamlRecord.Field("quoteIssuer", new Party(this.quoteIssuer)));
    fields.add(new DamlRecord.Field("quoteCurrency", new Text(this.quoteCurrency)));
    fields.add(new DamlRecord.Field("quoteAmount", new Numeric(this.quoteAmount)));
    return new DamlRecord(fields);
  }

  private static ValueDecoder<IouTrade> templateValueDecoder() throws IllegalArgumentException {
    return value$ -> {
      Value recordValue$ = value$;
      List<DamlRecord.Field> fields$ = PrimitiveValueDecoders.recordCheck(9, recordValue$);
      String buyer = PrimitiveValueDecoders.fromParty.decode(fields$.get(0).getValue());
      String seller = PrimitiveValueDecoders.fromParty.decode(fields$.get(1).getValue());
      Iou.ContractId baseIouCid =
          new Iou.ContractId(fields$.get(2).getValue().asContractId().orElseThrow(() -> new IllegalArgumentException("Expected baseIouCid to be of type com.daml.ledger.javaapi.data.ContractId")).getValue());
      String baseIssuer = PrimitiveValueDecoders.fromParty.decode(fields$.get(3).getValue());
      String baseCurrency = PrimitiveValueDecoders.fromText.decode(fields$.get(4).getValue());
      BigDecimal baseAmount = PrimitiveValueDecoders.fromNumeric.decode(fields$.get(5).getValue());
      String quoteIssuer = PrimitiveValueDecoders.fromParty.decode(fields$.get(6).getValue());
      String quoteCurrency = PrimitiveValueDecoders.fromText.decode(fields$.get(7).getValue());
      BigDecimal quoteAmount = PrimitiveValueDecoders.fromNumeric.decode(fields$.get(8).getValue());
      return new IouTrade(buyer, seller, baseIouCid, baseIssuer, baseCurrency, baseAmount,
          quoteIssuer, quoteCurrency, quoteAmount);
    } ;
  }

  public static ContractFilter<Contract> contractFilter() {
    return ContractFilter.of(COMPANION);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null) {
      return false;
    }
    if (!(object instanceof IouTrade)) {
      return false;
    }
    IouTrade other = (IouTrade) object;
    return Objects.equals(this.buyer, other.buyer) && Objects.equals(this.seller, other.seller) &&
        Objects.equals(this.baseIouCid, other.baseIouCid) &&
        Objects.equals(this.baseIssuer, other.baseIssuer) &&
        Objects.equals(this.baseCurrency, other.baseCurrency) &&
        Objects.equals(this.baseAmount, other.baseAmount) &&
        Objects.equals(this.quoteIssuer, other.quoteIssuer) &&
        Objects.equals(this.quoteCurrency, other.quoteCurrency) &&
        Objects.equals(this.quoteAmount, other.quoteAmount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.buyer, this.seller, this.baseIouCid, this.baseIssuer,
        this.baseCurrency, this.baseAmount, this.quoteIssuer, this.quoteCurrency, this.quoteAmount);
  }

  @Override
  public String toString() {
    return String.format("com.daml.quickstart.iou.ioutrade.IouTrade(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        this.buyer, this.seller, this.baseIouCid, this.baseIssuer, this.baseCurrency,
        this.baseAmount, this.quoteIssuer, this.quoteCurrency, this.quoteAmount);
  }

  public static final class ContractId extends com.daml.ledger.javaapi.data.codegen.ContractId<IouTrade> implements Exercises<ExerciseCommand> {
    public ContractId(String contractId) {
      super(contractId);
    }

    @Override
    protected ContractTypeCompanion<? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>, ContractId, IouTrade, ?> getCompanion(
        ) {
      return COMPANION;
    }

    public static ContractId fromContractId(
        com.daml.ledger.javaapi.data.codegen.ContractId<IouTrade> contractId) {
      return COMPANION.toContractId(contractId);
    }
  }

  public static class Contract extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, IouTrade> {
    public Contract(ContractId id, IouTrade data, Optional<String> agreementText,
        Set<String> signatories, Set<String> observers) {
      super(id, data, agreementText, signatories, observers);
    }

    @Override
    protected ContractCompanion<Contract, ContractId, IouTrade> getCompanion() {
      return COMPANION;
    }

    public static Contract fromIdAndRecord(String contractId, DamlRecord record$,
        Optional<String> agreementText, Set<String> signatories, Set<String> observers) {
      return COMPANION.fromIdAndRecord(contractId, record$, agreementText, signatories, observers);
    }

    public static Contract fromCreatedEvent(CreatedEvent event) {
      return COMPANION.fromCreatedEvent(event);
    }
  }

  public interface Exercises<Cmd> extends com.daml.ledger.javaapi.data.codegen.Exercises<Cmd> {
    default Update<Exercised<Tuple2<Iou.ContractId, Iou.ContractId>>> exerciseIouTrade_Accept(
        IouTrade_Accept arg) {
      return makeExerciseCmd(CHOICE_IouTrade_Accept, arg);
    }

    default Update<Exercised<Tuple2<Iou.ContractId, Iou.ContractId>>> exerciseIouTrade_Accept(
        Iou.ContractId quoteIouCid) {
      return exerciseIouTrade_Accept(new IouTrade_Accept(quoteIouCid));
    }

    default Update<Exercised<Unit>> exerciseArchive(Archive arg) {
      return makeExerciseCmd(CHOICE_Archive, arg);
    }

    default Update<Exercised<Unit>> exerciseTradeProposal_Reject(TradeProposal_Reject arg) {
      return makeExerciseCmd(CHOICE_TradeProposal_Reject, arg);
    }

    default Update<Exercised<Unit>> exerciseTradeProposal_Reject() {
      return exerciseTradeProposal_Reject(new TradeProposal_Reject());
    }
  }

  public static final class CreateAnd extends com.daml.ledger.javaapi.data.codegen.CreateAnd implements Exercises<CreateAndExerciseCommand> {
    CreateAnd(Template createArguments) {
      super(createArguments);
    }

    @Override
    protected ContractTypeCompanion<? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>, ContractId, IouTrade, ?> getCompanion(
        ) {
      return COMPANION;
    }
  }
}
