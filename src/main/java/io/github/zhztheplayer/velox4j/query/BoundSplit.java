package io.github.zhztheplayer.velox4j.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;

public class BoundSplit {
  private final String planNodeId;
  private final int groupId;
  private final ConnectorSplit split;

  @JsonCreator
  public BoundSplit(
      @JsonProperty("planNodeId") String planNodeId,
      @JsonProperty("groupId") int groupId,
      @JsonProperty("split") ConnectorSplit split) {
    this.planNodeId = Preconditions.checkNotNull(planNodeId);
    this.groupId = groupId;
    this.split = Preconditions.checkNotNull(split);
  }

  @JsonGetter("planNodeId")
  public String getPlanNodeId() {
    return planNodeId;
  }

  @JsonGetter("groupId")
  public int getGroupId() {
    return groupId;
  }

  @JsonGetter("split")
  public ConnectorSplit getSplit() {
    return split;
  }
}
