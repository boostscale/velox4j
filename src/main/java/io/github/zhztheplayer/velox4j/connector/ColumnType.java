package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ColumnType {
  PARTITION_KEY("PartitionKey"),
  REGULAR("Regular"),
  SYNTHESIZED("Synthesized"),
  ROW_INDEX("RowIndex");

  private final String value;

  ColumnType(String value) {
    this.value = value;
  }

  @JsonValue
  public String toValue() {
    return value;
  }
}
