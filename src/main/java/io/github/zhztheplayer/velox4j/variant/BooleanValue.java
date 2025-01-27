package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BooleanValue extends Variant {
  private final boolean value;

  @JsonCreator
  public BooleanValue(@JsonProperty("value") boolean value) {
    this.value = value;
  }

  @JsonGetter("value")
  public boolean isValue() {
    return value;
  }
}
