package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.serde.VeloxBean;

public abstract class Type extends VeloxBean {
  private final String type;

  @JsonCreator
  protected Type(String name, @JsonProperty("type") String type) {
    super(name);
    this.type = type;
  }

  @JsonGetter("type")
  public String getType() {
    return type;
  }
}
