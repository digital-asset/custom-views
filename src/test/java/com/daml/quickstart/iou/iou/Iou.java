// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou.iou;

import com.daml.ledger.javaapi.data.ContractFilter;
import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;
import com.daml.ledger.javaapi.data.CreateCommand;
import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.DamlCollectors;
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

public final class Iou extends Template {
  public static final Identifier TEMPLATE_ID = new Identifier("407382e4ffc06fed9636591a1196d8ce97961b51b1398fb9da5db679ec6e45fc", "Iou", "Iou");

  public static final Choice<Iou, Iou_AddObserver, ContractId> CHOICE_Iou_AddObserver = 
      Choice.create("Iou_AddObserver", value$ -> value$.toValue(), value$ ->
        Iou_AddObserver.valueDecoder().decode(value$), value$ ->
        new ContractId(value$.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()));

  public static final Choice<Iou, Iou_Split, Tuple2<ContractId, ContractId>> CHOICE_Iou_Split = 
      Choice.create("Iou_Split", value$ -> value$.toValue(), value$ -> Iou_Split.valueDecoder()
        .decode(value$), value$ -> Tuple2.<com.daml.quickstart.iou.iou.Iou.ContractId,
        com.daml.quickstart.iou.iou.Iou.ContractId>valueDecoder(v$0 ->
          new ContractId(v$0.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()),
        v$1 ->
          new ContractId(v$1.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()))
        .decode(value$));

  public static final Choice<Iou, Iou_RemoveObserver, ContractId> CHOICE_Iou_RemoveObserver = 
      Choice.create("Iou_RemoveObserver", value$ -> value$.toValue(), value$ ->
        Iou_RemoveObserver.valueDecoder().decode(value$), value$ ->
        new ContractId(value$.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()));

  public static final Choice<Iou, Iou_Transfer, IouTransfer.ContractId> CHOICE_Iou_Transfer = 
      Choice.create("Iou_Transfer", value$ -> value$.toValue(), value$ ->
        Iou_Transfer.valueDecoder().decode(value$), value$ ->
        new IouTransfer.ContractId(value$.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()));

  public static final Choice<Iou, Iou_Merge, ContractId> CHOICE_Iou_Merge = 
      Choice.create("Iou_Merge", value$ -> value$.toValue(), value$ -> Iou_Merge.valueDecoder()
        .decode(value$), value$ ->
        new ContractId(value$.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()));

  public static final Choice<Iou, Archive, Unit> CHOICE_Archive = 
      Choice.create("Archive", value$ -> value$.toValue(), value$ -> Archive.valueDecoder()
        .decode(value$), value$ -> PrimitiveValueDecoders.fromUnit.decode(value$));

  public static final ContractCompanion.WithoutKey<Contract, ContractId, Iou> COMPANION = 
      new ContractCompanion.WithoutKey<>("com.daml.quickstart.iou.iou.Iou", TEMPLATE_ID,
        ContractId::new, v -> Iou.templateValueDecoder().decode(v), Contract::new,
        List.of(CHOICE_Iou_Split, CHOICE_Iou_Merge, CHOICE_Iou_AddObserver, CHOICE_Archive,
        CHOICE_Iou_RemoveObserver, CHOICE_Iou_Transfer));

  public final String issuer;

  public final String owner;

  public final String currency;

  public final BigDecimal amount;

  public final List<String> observers;

  public Iou(String issuer, String owner, String currency, BigDecimal amount,
      List<String> observers) {
    this.issuer = issuer;
    this.owner = owner;
    this.currency = currency;
    this.amount = amount;
    this.observers = observers;
  }

  @Override
  public Update<Created<com.daml.ledger.javaapi.data.codegen.ContractId<Iou>>> create() {
    return new Update.CreateUpdate<com.daml.ledger.javaapi.data.codegen.ContractId<Iou>, Created<com.daml.ledger.javaapi.data.codegen.ContractId<Iou>>>(new CreateCommand(Iou.TEMPLATE_ID, this.toValue()), x -> x, ContractId::new);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_AddObserver} instead */
  @Deprecated
  public Update<Exercised<ContractId>> createAndExerciseIou_AddObserver(Iou_AddObserver arg) {
    return createAnd().exerciseIou_AddObserver(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_AddObserver} instead */
  @Deprecated
  public Update<Exercised<ContractId>> createAndExerciseIou_AddObserver(String newObserver) {
    return createAndExerciseIou_AddObserver(new Iou_AddObserver(newObserver));
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_Split} instead */
  @Deprecated
  public Update<Exercised<Tuple2<ContractId, ContractId>>> createAndExerciseIou_Split(
      Iou_Split arg) {
    return createAnd().exerciseIou_Split(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_Split} instead */
  @Deprecated
  public Update<Exercised<Tuple2<ContractId, ContractId>>> createAndExerciseIou_Split(
      BigDecimal splitAmount) {
    return createAndExerciseIou_Split(new Iou_Split(splitAmount));
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_RemoveObserver} instead */
  @Deprecated
  public Update<Exercised<ContractId>> createAndExerciseIou_RemoveObserver(Iou_RemoveObserver arg) {
    return createAnd().exerciseIou_RemoveObserver(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_RemoveObserver} instead */
  @Deprecated
  public Update<Exercised<ContractId>> createAndExerciseIou_RemoveObserver(String oldObserver) {
    return createAndExerciseIou_RemoveObserver(new Iou_RemoveObserver(oldObserver));
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_Transfer} instead */
  @Deprecated
  public Update<Exercised<IouTransfer.ContractId>> createAndExerciseIou_Transfer(Iou_Transfer arg) {
    return createAnd().exerciseIou_Transfer(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_Transfer} instead */
  @Deprecated
  public Update<Exercised<IouTransfer.ContractId>> createAndExerciseIou_Transfer(String newOwner) {
    return createAndExerciseIou_Transfer(new Iou_Transfer(newOwner));
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_Merge} instead */
  @Deprecated
  public Update<Exercised<ContractId>> createAndExerciseIou_Merge(Iou_Merge arg) {
    return createAnd().exerciseIou_Merge(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIou_Merge} instead */
  @Deprecated
  public Update<Exercised<ContractId>> createAndExerciseIou_Merge(ContractId otherCid) {
    return createAndExerciseIou_Merge(new Iou_Merge(otherCid));
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseArchive} instead */
  @Deprecated
  public Update<Exercised<Unit>> createAndExerciseArchive(Archive arg) {
    return createAnd().exerciseArchive(arg);
  }

  public static Update<Created<com.daml.ledger.javaapi.data.codegen.ContractId<Iou>>> create(
      String issuer, String owner, String currency, BigDecimal amount, List<String> observers) {
    return new Iou(issuer, owner, currency, amount, observers).create();
  }

  @Override
  public CreateAnd createAnd() {
    return new CreateAnd(this);
  }

  @Override
  protected ContractCompanion.WithoutKey<Contract, ContractId, Iou> getCompanion() {
    return COMPANION;
  }

  /**
   * @deprecated since Daml 2.5.0; use {@code valueDecoder} instead */
  @Deprecated
  public static Iou fromValue(Value value$) throws IllegalArgumentException {
    return valueDecoder().decode(value$);
  }

  public static ValueDecoder<Iou> valueDecoder() throws IllegalArgumentException {
    return ContractCompanion.valueDecoder(COMPANION);
  }

  public DamlRecord toValue() {
    ArrayList<DamlRecord.Field> fields = new ArrayList<DamlRecord.Field>(5);
    fields.add(new DamlRecord.Field("issuer", new Party(this.issuer)));
    fields.add(new DamlRecord.Field("owner", new Party(this.owner)));
    fields.add(new DamlRecord.Field("currency", new Text(this.currency)));
    fields.add(new DamlRecord.Field("amount", new Numeric(this.amount)));
    fields.add(new DamlRecord.Field("observers", this.observers.stream().collect(DamlCollectors.toDamlList(v$0 -> new Party(v$0)))));
    return new DamlRecord(fields);
  }

  private static ValueDecoder<Iou> templateValueDecoder() throws IllegalArgumentException {
    return value$ -> {
      Value recordValue$ = value$;
      List<DamlRecord.Field> fields$ = PrimitiveValueDecoders.recordCheck(5, recordValue$);
      String issuer = PrimitiveValueDecoders.fromParty.decode(fields$.get(0).getValue());
      String owner = PrimitiveValueDecoders.fromParty.decode(fields$.get(1).getValue());
      String currency = PrimitiveValueDecoders.fromText.decode(fields$.get(2).getValue());
      BigDecimal amount = PrimitiveValueDecoders.fromNumeric.decode(fields$.get(3).getValue());
      List<String> observers = PrimitiveValueDecoders.fromList(PrimitiveValueDecoders.fromParty)
          .decode(fields$.get(4).getValue());
      return new Iou(issuer, owner, currency, amount, observers);
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
    if (!(object instanceof Iou)) {
      return false;
    }
    Iou other = (Iou) object;
    return Objects.equals(this.issuer, other.issuer) && Objects.equals(this.owner, other.owner) &&
        Objects.equals(this.currency, other.currency) &&
        Objects.equals(this.amount, other.amount) &&
        Objects.equals(this.observers, other.observers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.issuer, this.owner, this.currency, this.amount, this.observers);
  }

  @Override
  public String toString() {
    return String.format("com.daml.quickstart.iou.iou.Iou(%s, %s, %s, %s, %s)", this.issuer,
        this.owner, this.currency, this.amount, this.observers);
  }

  public static final class ContractId extends com.daml.ledger.javaapi.data.codegen.ContractId<Iou> implements Exercises<ExerciseCommand> {
    public ContractId(String contractId) {
      super(contractId);
    }

    @Override
    protected ContractTypeCompanion<? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>, ContractId, Iou, ?> getCompanion(
        ) {
      return COMPANION;
    }

    public static ContractId fromContractId(
        com.daml.ledger.javaapi.data.codegen.ContractId<Iou> contractId) {
      return COMPANION.toContractId(contractId);
    }
  }

  public static class Contract extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, Iou> {
    public Contract(ContractId id, Iou data, Optional<String> agreementText,
        Set<String> signatories, Set<String> observers) {
      super(id, data, agreementText, signatories, observers);
    }

    @Override
    protected ContractCompanion<Contract, ContractId, Iou> getCompanion() {
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
    default Update<Exercised<ContractId>> exerciseIou_AddObserver(Iou_AddObserver arg) {
      return makeExerciseCmd(CHOICE_Iou_AddObserver, arg);
    }

    default Update<Exercised<ContractId>> exerciseIou_AddObserver(String newObserver) {
      return exerciseIou_AddObserver(new Iou_AddObserver(newObserver));
    }

    default Update<Exercised<Tuple2<ContractId, ContractId>>> exerciseIou_Split(Iou_Split arg) {
      return makeExerciseCmd(CHOICE_Iou_Split, arg);
    }

    default Update<Exercised<Tuple2<ContractId, ContractId>>> exerciseIou_Split(
        BigDecimal splitAmount) {
      return exerciseIou_Split(new Iou_Split(splitAmount));
    }

    default Update<Exercised<ContractId>> exerciseIou_RemoveObserver(Iou_RemoveObserver arg) {
      return makeExerciseCmd(CHOICE_Iou_RemoveObserver, arg);
    }

    default Update<Exercised<ContractId>> exerciseIou_RemoveObserver(String oldObserver) {
      return exerciseIou_RemoveObserver(new Iou_RemoveObserver(oldObserver));
    }

    default Update<Exercised<IouTransfer.ContractId>> exerciseIou_Transfer(Iou_Transfer arg) {
      return makeExerciseCmd(CHOICE_Iou_Transfer, arg);
    }

    default Update<Exercised<IouTransfer.ContractId>> exerciseIou_Transfer(String newOwner) {
      return exerciseIou_Transfer(new Iou_Transfer(newOwner));
    }

    default Update<Exercised<ContractId>> exerciseIou_Merge(Iou_Merge arg) {
      return makeExerciseCmd(CHOICE_Iou_Merge, arg);
    }

    default Update<Exercised<ContractId>> exerciseIou_Merge(ContractId otherCid) {
      return exerciseIou_Merge(new Iou_Merge(otherCid));
    }

    default Update<Exercised<Unit>> exerciseArchive(Archive arg) {
      return makeExerciseCmd(CHOICE_Archive, arg);
    }
  }

  public static final class CreateAnd extends com.daml.ledger.javaapi.data.codegen.CreateAnd implements Exercises<CreateAndExerciseCommand> {
    CreateAnd(Template createArguments) {
      super(createArguments);
    }

    @Override
    protected ContractTypeCompanion<? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>, ContractId, Iou, ?> getCompanion(
        ) {
      return COMPANION;
    }
  }
}
