package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

public class LocationHandle extends ISerializable {
  private final String targetPath;
  private final String writePath;
  private final TableType tableType;
  private final String targetFileName;

  @JsonCreator
  public LocationHandle(
      @JsonProperty("targetPath") String targetPath,
      @JsonProperty("writePath") String writePath,
      @JsonProperty("tableType") TableType tableType,
      @JsonProperty("targetFileName") String targetFileName) {
    this.targetPath = targetPath;
    this.writePath = writePath;
    this.tableType = tableType;
    this.targetFileName = targetFileName;
  }

  @JsonGetter("targetPath")
  public String getTargetPath() {
    return targetPath;
  }

  @JsonGetter("writePath")
  public String getWritePath() {
    return writePath;
  }

  @JsonGetter("tableType")
  public TableType getTableType() {
    return tableType;
  }

  @JsonGetter("targetFileName")
  public String getTargetFileName() {
    return targetFileName;
  }

  public enum TableType {
    NEW("kNew"),
    EXISTING("kExisting");

    private final String value;

    TableType(String value) {
      this.value = value;
    }

    @JsonValue
    public String toValue() {
      return value;
    }
  }
}
