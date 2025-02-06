package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RealValue realValue = (RealValue) o;
    return Float.compare(value, realValue.value) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public String toString() {
    return "RealValue{" +
        "value=" + value +
        '}';
  }
}