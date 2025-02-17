package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class DoubleValue extends Variant {
  private final double value;

  @JsonCreator
  public DoubleValue(@JsonProperty("value") double value) {
    this.value = value;
  }

  @JsonGetter("value")
  public double getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DoubleValue that = (DoubleValue) o;
    return Double.compare(value, that.value) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public String toString() {
    return "DoubleValue{" +
        "value=" + value +
        '}';
  }
}