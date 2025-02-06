package io.github.zhztheplayer.velox4j.aggregate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AggregateStep {
  PARTIAL("PARTIAL"),
  FINAL("FINAL"),
  INTERMEDIATE("INTERMEDIATE"),
  SINGLE("SINGLE");

  private final String value;

  @JsonCreator
  AggregateStep(String value) {
    this.value = value;
  }

  @JsonValue
  public String toValue() {
    return value;
  }
}
