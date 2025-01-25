package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OpaqueType extends Type {
  private final String opaque;

  @JsonCreator
  public OpaqueType(@JsonProperty("opaque") String opaque) {
    this.opaque = opaque;
  }

  @JsonGetter("opaque")
  public String getOpaque() {
    return opaque;
  }
}
