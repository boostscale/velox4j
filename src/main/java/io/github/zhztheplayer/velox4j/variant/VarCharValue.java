package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VarCharValue that = (VarCharValue) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public String toString() {
    return "VarCharValue{" +
        "value='" + value + '\'' +
        '}';
  }
}
