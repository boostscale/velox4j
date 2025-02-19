package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonValue;

public enum FileFormat {
  UNKNOWN("unknown"),
  DWRF("dwrf"),
  RC("rc"),
  RC_TEXT("rc:text"),
  RC_BINARY("rc:binary"),
  TEXT("text"),
  JSON("json"),
  PARQUET("parquet"),
  NIMBLE("nimble"),
  ORC("orc"),
  SST("sst");

  private final String value;

  FileFormat(String value) {
    this.value = value;
  }

  @JsonValue
  public String toValue() {
    return value;
  }
}
