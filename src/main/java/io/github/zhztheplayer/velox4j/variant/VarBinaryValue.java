package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Base64;

public class VarBinaryValue extends Variant {
  private final String value;

  @JsonCreator
  private VarBinaryValue(@JsonProperty("value") String value) {
    this.value = value;
  }

  public static VarBinaryValue create(byte[] value) {
    return new VarBinaryValue(Base64.getEncoder().encodeToString(value));
  }

  @JsonGetter("value")
  public String getValue() {
    return value;
  }
}
