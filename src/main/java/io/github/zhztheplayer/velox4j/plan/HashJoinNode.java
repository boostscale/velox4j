package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.type.RowType;

import java.util.List;

public class HashJoinNode extends AbstractJoinNode {
  private final boolean nullAware;

  public HashJoinNode(
      String id,
      JoinType joinType,
      List<FieldAccessTypedExpr> leftKeys,
      List<FieldAccessTypedExpr> rightKeys,
      TypedExpr filter,
      PlanNode left,
      PlanNode right,
      RowType outputType,
      boolean nullAware) {
    super(id, joinType, leftKeys, rightKeys, filter, left, right, outputType);
    this.nullAware = nullAware;
  }

  @JsonCreator
  private static HashJoinNode create(
      @JsonProperty("id") String id,
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("leftKeys") List<FieldAccessTypedExpr> leftKeys,
      @JsonProperty("rightKeys") List<FieldAccessTypedExpr> rightKeys,
      @JsonProperty("filter") TypedExpr filter,
      @JsonProperty("sources") List<PlanNode> sources,
      @JsonProperty("outputType") RowType outputType,
      @JsonProperty("nullAware") boolean nullAware) {
    Preconditions.checkArgument(sources.size() == 2,
        "HashJoinNode should have 2 sources, but has %s", sources.size());
    return new HashJoinNode(id, joinType, leftKeys, rightKeys, filter, sources.get(0),
        sources.get(1), outputType, nullAware);
  }

  @JsonGetter("nullAware")
  public boolean isNullAware() {
    return nullAware;
  }
}
