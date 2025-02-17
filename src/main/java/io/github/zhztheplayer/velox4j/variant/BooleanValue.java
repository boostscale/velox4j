package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class BooleanValue extends Variant {
  private final boolean value;

  @JsonCreator
  public BooleanValue(@JsonProperty("value") boolean value) {
    this.value = value;
  }

  @JsonGetter("value")
  public boolean getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BooleanValue that = (BooleanValue) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public String toString() {
    return "BooleanValue{" +
        "value=" + value +
        '}';
  }
}
