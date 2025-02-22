package io.github.zhztheplayer.velox4j.sort;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SortOrder {
  private final boolean ascending;
  private final boolean nullsFirst;

  @JsonCreator
  public SortOrder(
      @JsonProperty("ascending") boolean ascending,
      @JsonProperty("nullsFirst") boolean nullsFirst
  ) {
    this.ascending = ascending;
    this.nullsFirst = nullsFirst;
  }

  @JsonProperty("ascending")
  public boolean isAscending() {
    return ascending;
  }

  @JsonProperty("nullsFirst")
  public boolean isNullsFirst() {
    return nullsFirst;
  }
}
