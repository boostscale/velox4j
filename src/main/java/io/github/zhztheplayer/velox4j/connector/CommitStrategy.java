package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonValue;

public enum CommitStrategy {
  NO_COMMIT("NO_COMMIT"),
  TASK_COMMIT("TASK_COMMIT");

  private final String value;

  CommitStrategy(String value) {
    this.value = value;
  }

  @JsonValue
  public String toValue() {
    return value;
  }
}
