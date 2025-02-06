package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;
import java.util.Objects;

public class HugeIntValue extends Variant {
  private final BigInteger value;

  @JsonCreator
  public HugeIntValue(@JsonProperty("value") BigInteger value) {
    this.value = value;
  }

  @JsonGetter("value")
  public BigInteger getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HugeIntValue that = (HugeIntValue) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

    @Override
    public String toString() {
        return "HugeIntValue{" +
            "value=" + value +
            '}';
    }
}
