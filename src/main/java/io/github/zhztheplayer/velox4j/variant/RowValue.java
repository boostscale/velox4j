package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class RowValue extends Variant {
  private final List<Variant> row;

  @JsonCreator
  public RowValue(@JsonProperty("value") List<Variant> row) {
    this.row = Collections.unmodifiableList(row);
  }

  @JsonGetter("value")
  public List<Variant> getRow() {
    return row;
  }
}
