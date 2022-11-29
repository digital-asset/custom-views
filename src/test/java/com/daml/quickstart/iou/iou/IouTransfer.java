// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.quickstart.iou.iou;

import com.daml.ledger.javaapi.data.ContractFilter;
import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;
import com.daml.ledger.javaapi.data.CreateCommand;
import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.ExerciseCommand;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.Template;
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
import java.lang.Deprecated;
import java.lang.IllegalArgumentException;
import java.lang.Object;
import java.lang.Override;
import java.lang.String;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class IouTransfer extends Template {
  public static final Identifier TEMPLATE_ID = new Identifier("407382e4ffc06fed9636591a1196d8ce97961b51b1398fb9da5db679ec6e45fc", "Iou", "IouTransfer");

  public static final Choice<IouTransfer, IouTransfer_Cancel, Iou.ContractId> CHOICE_IouTransfer_Cancel = 
      Choice.create("IouTransfer_Cancel", value$ -> value$.toValue(), value$ ->
        IouTransfer_Cancel.valueDecoder().decode(value$), value$ ->
        new Iou.ContractId(value$.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()));

  public static final Choice<IouTransfer, IouTransfer_Reject, Iou.ContractId> CHOICE_IouTransfer_Reject = 
      Choice.create("IouTransfer_Reject", value$ -> value$.toValue(), value$ ->
        IouTransfer_Reject.valueDecoder().decode(value$), value$ ->
        new Iou.ContractId(value$.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()));

  public static final Choice<IouTransfer, Archive, Unit> CHOICE_Archive = 
      Choice.create("Archive", value$ -> value$.toValue(), value$ -> Archive.valueDecoder()
        .decode(value$), value$ -> PrimitiveValueDecoders.fromUnit.decode(value$));

  public static final Choice<IouTransfer, IouTransfer_Accept, Iou.ContractId> CHOICE_IouTransfer_Accept = 
      Choice.create("IouTransfer_Accept", value$ -> value$.toValue(), value$ ->
        IouTransfer_Accept.valueDecoder().decode(value$), value$ ->
        new Iou.ContractId(value$.asContractId().orElseThrow(() -> new IllegalArgumentException("Expected value$ to be of type com.daml.ledger.javaapi.data.ContractId")).getValue()));

  public static final ContractCompanion.WithoutKey<Contract, ContractId, IouTransfer> COMPANION = 
      new ContractCompanion.WithoutKey<>("com.daml.quickstart.iou.iou.IouTransfer", TEMPLATE_ID,
        ContractId::new, v -> IouTransfer.templateValueDecoder().decode(v), Contract::new,
        List.of(CHOICE_IouTransfer_Cancel, CHOICE_IouTransfer_Reject, CHOICE_Archive,
        CHOICE_IouTransfer_Accept));

  public final Iou iou;

  public final String newOwner;

  public IouTransfer(Iou iou, String newOwner) {
    this.iou = iou;
    this.newOwner = newOwner;
  }

  @Override
  public Update<Created<com.daml.ledger.javaapi.data.codegen.ContractId<IouTransfer>>> create() {
    return new Update.CreateUpdate<com.daml.ledger.javaapi.data.codegen.ContractId<IouTransfer>, Created<com.daml.ledger.javaapi.data.codegen.ContractId<IouTransfer>>>(new CreateCommand(IouTransfer.TEMPLATE_ID, this.toValue()), x -> x, ContractId::new);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIouTransfer_Cancel} instead */
  @Deprecated
  public Update<Exercised<Iou.ContractId>> createAndExerciseIouTransfer_Cancel(
      IouTransfer_Cancel arg) {
    return createAnd().exerciseIouTransfer_Cancel(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIouTransfer_Cancel} instead */
  @Deprecated
  public Update<Exercised<Iou.ContractId>> createAndExerciseIouTransfer_Cancel() {
    return createAndExerciseIouTransfer_Cancel(new IouTransfer_Cancel());
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIouTransfer_Reject} instead */
  @Deprecated
  public Update<Exercised<Iou.ContractId>> createAndExerciseIouTransfer_Reject(
      IouTransfer_Reject arg) {
    return createAnd().exerciseIouTransfer_Reject(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIouTransfer_Reject} instead */
  @Deprecated
  public Update<Exercised<Iou.ContractId>> createAndExerciseIouTransfer_Reject() {
    return createAndExerciseIouTransfer_Reject(new IouTransfer_Reject());
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseArchive} instead */
  @Deprecated
  public Update<Exercised<Unit>> createAndExerciseArchive(Archive arg) {
    return createAnd().exerciseArchive(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIouTransfer_Accept} instead */
  @Deprecated
  public Update<Exercised<Iou.ContractId>> createAndExerciseIouTransfer_Accept(
      IouTransfer_Accept arg) {
    return createAnd().exerciseIouTransfer_Accept(arg);
  }

  /**
   * @deprecated since Daml 2.3.0; use {@code createAnd().exerciseIouTransfer_Accept} instead */
  @Deprecated
  public Update<Exercised<Iou.ContractId>> createAndExerciseIouTransfer_Accept() {
    return createAndExerciseIouTransfer_Accept(new IouTransfer_Accept());
  }

  public static Update<Created<com.daml.ledger.javaapi.data.codegen.ContractId<IouTransfer>>> create(
      Iou iou, String newOwner) {
    return new IouTransfer(iou, newOwner).create();
  }

  @Override
  public CreateAnd createAnd() {
    return new CreateAnd(this);
  }

  @Override
  protected ContractCompanion.WithoutKey<Contract, ContractId, IouTransfer> getCompanion() {
    return COMPANION;
  }

  /**
   * @deprecated since Daml 2.5.0; use {@code valueDecoder} instead */
  @Deprecated
  public static IouTransfer fromValue(Value value$) throws IllegalArgumentException {
    return valueDecoder().decode(value$);
  }

  public static ValueDecoder<IouTransfer> valueDecoder() throws IllegalArgumentException {
    return ContractCompanion.valueDecoder(COMPANION);
  }

  public DamlRecord toValue() {
    ArrayList<DamlRecord.Field> fields = new ArrayList<DamlRecord.Field>(2);
    fields.add(new DamlRecord.Field("iou", this.iou.toValue()));
    fields.add(new DamlRecord.Field("newOwner", new Party(this.newOwner)));
    return new DamlRecord(fields);
  }

  private static ValueDecoder<IouTransfer> templateValueDecoder() throws IllegalArgumentException {
    return value$ -> {
      Value recordValue$ = value$;
      List<DamlRecord.Field> fields$ = PrimitiveValueDecoders.recordCheck(2, recordValue$);
      Iou iou = Iou.valueDecoder().decode(fields$.get(0).getValue());
      String newOwner = PrimitiveValueDecoders.fromParty.decode(fields$.get(1).getValue());
      return new IouTransfer(iou, newOwner);
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
    if (!(object instanceof IouTransfer)) {
      return false;
    }
    IouTransfer other = (IouTransfer) object;
    return Objects.equals(this.iou, other.iou) && Objects.equals(this.newOwner, other.newOwner);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.iou, this.newOwner);
  }

  @Override
  public String toString() {
    return String.format("com.daml.quickstart.iou.iou.IouTransfer(%s, %s)", this.iou,
        this.newOwner);
  }

  public static final class ContractId extends com.daml.ledger.javaapi.data.codegen.ContractId<IouTransfer> implements Exercises<ExerciseCommand> {
    public ContractId(String contractId) {
      super(contractId);
    }

    @Override
    protected ContractTypeCompanion<? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>, ContractId, IouTransfer, ?> getCompanion(
        ) {
      return COMPANION;
    }

    public static ContractId fromContractId(
        com.daml.ledger.javaapi.data.codegen.ContractId<IouTransfer> contractId) {
      return COMPANION.toContractId(contractId);
    }
  }

  public static class Contract extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, IouTransfer> {
    public Contract(ContractId id, IouTransfer data, Optional<String> agreementText,
        Set<String> signatories, Set<String> observers) {
      super(id, data, agreementText, signatories, observers);
    }

    @Override
    protected ContractCompanion<Contract, ContractId, IouTransfer> getCompanion() {
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
    default Update<Exercised<Iou.ContractId>> exerciseIouTransfer_Cancel(IouTransfer_Cancel arg) {
      return makeExerciseCmd(CHOICE_IouTransfer_Cancel, arg);
    }

    default Update<Exercised<Iou.ContractId>> exerciseIouTransfer_Cancel() {
      return exerciseIouTransfer_Cancel(new IouTransfer_Cancel());
    }

    default Update<Exercised<Iou.ContractId>> exerciseIouTransfer_Reject(IouTransfer_Reject arg) {
      return makeExerciseCmd(CHOICE_IouTransfer_Reject, arg);
    }

    default Update<Exercised<Iou.ContractId>> exerciseIouTransfer_Reject() {
      return exerciseIouTransfer_Reject(new IouTransfer_Reject());
    }

    default Update<Exercised<Unit>> exerciseArchive(Archive arg) {
      return makeExerciseCmd(CHOICE_Archive, arg);
    }

    default Update<Exercised<Iou.ContractId>> exerciseIouTransfer_Accept(IouTransfer_Accept arg) {
      return makeExerciseCmd(CHOICE_IouTransfer_Accept, arg);
    }

    default Update<Exercised<Iou.ContractId>> exerciseIouTransfer_Accept() {
      return exerciseIouTransfer_Accept(new IouTransfer_Accept());
    }
  }

  public static final class CreateAnd extends com.daml.ledger.javaapi.data.codegen.CreateAnd implements Exercises<CreateAndExerciseCommand> {
    CreateAnd(Template createArguments) {
      super(createArguments);
    }

    @Override
    protected ContractTypeCompanion<? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>, ContractId, IouTransfer, ?> getCompanion(
        ) {
      return COMPANION;
    }
  }
}
