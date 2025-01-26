package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.RowType;

import java.util.List;
import java.util.Map;

public class HiveTableHandle extends ConnectorTableHandle {
  private final String tableName;
  private final boolean filterPushdownEnabled;
  private final List<SubfieldFilter> subfieldFilters;
  private final TypedExpr remainingFilter;
  private final RowType dataColumns;
  private final Map<String, String> tableParameters;

  @JsonCreator
  public HiveTableHandle(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("filterPushdownEnabled") boolean filterPushdownEnabled,
      @JsonProperty("subfieldFilters") List<SubfieldFilter> subfieldFilters,
      @JsonProperty("remainingFilter") TypedExpr remainingFilter,
      @JsonProperty("dataColumns") RowType dataColumns,
      @JsonProperty("tableParameters") Map<String, String> tableParameters) {
    super(connectorId);
    this.tableName = tableName;
    this.filterPushdownEnabled = filterPushdownEnabled;
    this.subfieldFilters = subfieldFilters;
    this.remainingFilter = remainingFilter;
    this.dataColumns = dataColumns;
    this.tableParameters = tableParameters;
  }

  @JsonGetter("tableName")
  public String getTableName() {
    return tableName;
  }

  @JsonGetter("filterPushdownEnabled")
  public boolean isFilterPushdownEnabled() {
    return filterPushdownEnabled;
  }

  @JsonGetter("subfieldFilters")
  public List<SubfieldFilter> getSubfieldFilters() {
    return subfieldFilters;
  }

  @JsonGetter("remainingFilter")
  public TypedExpr getRemainingFilter() {
    return remainingFilter;
  }

  @JsonGetter("dataColumns")
  public RowType getDataColumns() {
    return dataColumns;
  }

  @JsonIgnore // FIXME: This field doesn't involve the serialization in Velox for now.
  @JsonGetter("tableParameters")
  public Map<String, String> getTableParameters() {
    return tableParameters;
  }
}
