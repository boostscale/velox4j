package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.filter.Filter;

public class SubfieldFilter {
  private final String subfield;
  private final Filter filter;

  @JsonCreator
  public SubfieldFilter(@JsonProperty("subfield") String subfield,
      @JsonProperty("filter") Filter filter) {
    this.subfield = subfield;
    this.filter = filter;
  }

  @JsonGetter("subfield")
  public String getSubfield() {
    return subfield;
  }

  @JsonGetter("filter")
  public Filter getFilter() {
    return filter;
  }
}
