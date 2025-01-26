package io.github.zhztheplayer.velox4j.split;

import com.fasterxml.jackson.annotation.JsonValue;

public enum FileFormat {
  UNKNOWN(0),
  DWRF(1),
  RC(2),
  RC_TEXT(3),
  RC_BINARY(4),
  TEXT(5),
  JSON(6),
  PARQUET(7),
  NIMBLE(8),
  ORC(9),
  SST(10);

  private final int id;

  FileFormat(int id) {
    this.id = id;
  }

  @JsonValue
  public int toValue() {
    return id;
  }
}
