package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class LimitNode extends PlanNode {
  private final List<PlanNode> sources;
  private final long offset;
  private final long count;
  private final boolean partial;

  @JsonCreator
  public LimitNode(
      @JsonProperty("id") String id,
      @JsonProperty("sources") List<PlanNode> sources,
      @JsonProperty("offset") long offset,
      @JsonProperty("count") long count,
      @JsonProperty("partial") boolean partial) {
    super(id);
    this.sources = sources;
    this.offset = offset;
    this.count = count;
    this.partial = partial;
  }

  @Override
  protected List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("offset")
  public long getOffset() {
    return offset;
  }

  @JsonGetter("count")
  public long getCount() {
    return count;
  }

  @JsonGetter("partial")
  public boolean isPartial() {
    return partial;
  }
}
