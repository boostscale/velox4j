package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonValue;

public enum CompressionKind {
  NONE("none"),
  ZLIB("zlib"),
  SNAPPY("snappy"),
  LZO("lzo"),
  ZSTD("zstd"),
  LZ4("lz4"),
  GZIP("gzip");

  private final String value;

  CompressionKind(String value) {
    this.value = value;
  }

  @JsonValue
  public String toValue() {
    return value;
  }
}
