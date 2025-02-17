package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;

import java.util.List;

public class FilterNode extends PlanNode {
  private final List<PlanNode> sources;
  private final TypedExpr filter;

  @JsonCreator
  public FilterNode(
      @JsonProperty("id") String id,
      @JsonProperty("sources") List<PlanNode> sources,
      @JsonProperty("filter") TypedExpr filter) {
    super(id);
    this.sources = sources;
    this.filter = filter;
  }

  @Override
  protected List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("filter")
  public TypedExpr getFilter() {
    return filter;
  }
}
