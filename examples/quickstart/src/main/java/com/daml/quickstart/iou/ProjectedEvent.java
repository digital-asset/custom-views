package com.daml.quickstart.iou;

public final class ProjectedEvent {
  private String eventId;
  private String contractId;
  private String currency;
  private Double amount;

  public ProjectedEvent(String eventId, String contractId, String currency, Double amount) {
    this.eventId = eventId;
    this.contractId = contractId;
    this.currency = currency;
    this.amount = amount;
  }

  public String getEventId() {
    return eventId;
  }

  public void setEventId(String eventId) {
    this.eventId = eventId;
  }

  public String getContractId() {
    return contractId;
  }

  public void setContractId(String contractId) {
    this.contractId = contractId;
  }

  public String getCurrency() {
    return currency;
  }

  public void setCurrency(String currency) {
    this.currency = currency;
  }

  public Double getAmount() {
    return amount;
  }

  public void setAmount(Double amount) {
    this.amount = amount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ProjectedEvent that = (ProjectedEvent) o;

    if (!eventId.equals(that.eventId)) return false;
    if (!contractId.equals(that.contractId)) return false;
    if (!currency.equals(that.currency)) return false;
    return amount.equals(that.amount);
  }

  @Override
  public int hashCode() {
    int result = eventId.hashCode();
    result = 31 * result + contractId.hashCode();
    result = 31 * result + currency.hashCode();
    result = 31 * result + amount.hashCode();
    return result;
  }
}
