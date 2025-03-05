package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

import java.util.List;

public abstract class PlanNode extends ISerializable {
  private final String id;

  protected PlanNode(String id) {
    this.id = id;
  }

  @JsonGetter("id")
  public String getId() {
    return id;
  }

  @JsonGetter("sources")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  protected abstract List<PlanNode> getSources();
}
