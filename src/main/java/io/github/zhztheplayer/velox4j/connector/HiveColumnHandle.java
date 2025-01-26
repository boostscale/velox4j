package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public class HiveColumnHandle extends ColumnHandle {
  private final String name;
  private final ColumnType columnType;
  private final Type dataType;
  private final Type hiveType;
  private final List<String> requiredSubfields;

  @JsonCreator
  public HiveColumnHandle(
      @JsonProperty("hiveColumnHandleName") String name,
      @JsonProperty("columnType") ColumnType columnType,
      @JsonProperty("dataType") Type dataType,
      @JsonProperty("hiveType") Type hiveType,
      @JsonProperty("requiredSubfields") List<String> requiredSubfields) {
    this.name = name;
    this.columnType = columnType;
    this.dataType = dataType;
    this.hiveType = hiveType;
    this.requiredSubfields = requiredSubfields;
  }

  @JsonGetter("hiveColumnHandleName")
  public String getName() {
    return name;
  }

  @JsonGetter("columnType")
  public ColumnType getColumnType() {
    return columnType;
  }

  @JsonGetter("dataType")
  public Type getDataType() {
    return dataType;
  }

  @JsonGetter("hiveType")
  public Type getHiveType() {
    return hiveType;
  }

  @JsonGetter("requiredSubfields")
  public List<String> getRequiredSubfields() {
    return requiredSubfields;
  }
}
