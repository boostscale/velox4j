package io.github.zhztheplayer.velox4j.join;

import com.fasterxml.jackson.annotation.JsonValue;

public enum JoinType {
  INNER("INNER"),
  LEFT("LEFT"),
  RIGHT("RIGHT"),
  FULL("FULL"),
  LEFT_SEMI_FILTER("LEFT SEMI (FILTER)"),
  RIGHT_SEMI_FILTER("RIGHT SEMI (FILTER)"),
  LEFT_SEMI_PROJECT("LEFT SEMI (PROJECT)"),
  RIGHT_SEMI_PROJECT("RIGHT SEMI (PROJECT)"),
  ANTI("ANTI");

  private final String value;

  JoinType(String value) {
    this.value = value;
  }

  @JsonValue
  public String toValue() {
    return value;
  }

}
