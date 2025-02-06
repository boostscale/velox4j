package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ArrayValue extends Variant {
  private final List<Variant> array;

  @JsonCreator
  public ArrayValue(@JsonProperty("value") List<Variant> array) {
    Variants.checkSameType(array);
    this.array = Collections.unmodifiableList(array);
  }

  @JsonGetter("value")
  public List<Variant> getArray() {
    return array;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrayValue that = (ArrayValue) o;
    return Objects.equals(array, that.array);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(array);
  }
}
