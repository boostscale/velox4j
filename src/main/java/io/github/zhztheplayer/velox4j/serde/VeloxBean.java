package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class VeloxBean {
  private final String key;

  @JsonCreator
  public VeloxBean(@JsonProperty("name") String key) {
    this.key = key;
  }

  @JsonGetter("name")
  public String getKey() {
    return key;
  }
}
