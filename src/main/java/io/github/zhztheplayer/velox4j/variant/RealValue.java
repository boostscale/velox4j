package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RealValue extends Variant {
  private final float value;

  @JsonCreator
  public RealValue(@JsonProperty("value") float value) {
    this.value = value;
  }

  @JsonGetter("value")
  public float getValue() {
    return value;
  }
}