package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class IntegerValue extends Variant {
  private final int value;

  @JsonCreator
  public IntegerValue(@JsonProperty("value") int value) {
    this.value = value;
  }

  @JsonGetter("value")
  public int getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IntegerValue that = (IntegerValue) o;
    return value == that.value;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }
}