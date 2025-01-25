package io.github.zhztheplayer.velox4j.serde;

import com.fasterxml.jackson.annotation.JsonGetter;

public abstract class VeloxBean {
  private final String key;

  protected VeloxBean() {
    key = VeloxBeanRegistry.findKeyByClass(this.getClass());
  }

  @JsonGetter("name")
  public String getKey() {
    return key;
  }
}
