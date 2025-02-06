package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VarCharValue extends Variant {
  private final String value;

  @JsonCreator
  public VarCharValue(@JsonProperty("value") String value) {
    this.value = value;
  }

  @JsonGetter("value")
  public String getValue() {
    return value;
  }
}
