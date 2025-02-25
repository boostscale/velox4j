package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;
import io.github.zhztheplayer.velox4j.sort.SortOrder;

public class HiveSortingColumn extends ISerializable {
  private final String sortColumn;
  private final SortOrder sortOrder;

  @JsonCreator
  public HiveSortingColumn(
      @JsonProperty("columnName") String sortColumn,
      @JsonProperty("sortOrder") SortOrder sortOrder
  ) {
    this.sortColumn = sortColumn;
    this.sortOrder = sortOrder;
  }

  @JsonGetter("columnName")
  public String getSortColumn() {
    return sortColumn;
  }

  @JsonGetter("sortOrder")
  public SortOrder getSortOrder() {
    return sortOrder;
  }
}
