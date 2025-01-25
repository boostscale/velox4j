package io.github.zhztheplayer.velox4j.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.bean.VeloxBean;

public abstract class Type extends VeloxBean {
  private final String type;

  @JsonCreator
  protected Type(@JsonProperty("type") String type) {
    this.type = type;
  }

  @JsonGetter("type")
  public String getType() {
    return type;
  }
}
