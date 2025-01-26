package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonGetter;
import io.github.zhztheplayer.velox4j.bean.VeloxBean;

public abstract class PlanNode extends VeloxBean {
  private final String id;

  protected PlanNode(String id) {
    this.id = id;
  }

  @JsonGetter("id")
  public String getId() {
    return id;
  }
}
