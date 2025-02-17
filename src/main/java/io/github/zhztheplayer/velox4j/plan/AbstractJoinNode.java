package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonGetter;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.type.RowType;

import java.util.List;

public abstract class AbstractJoinNode extends PlanNode {
  private final List<PlanNode> sources;
  private final JoinType joinType;
  private final List<FieldAccessTypedExpr> leftKeys;
  private final List<FieldAccessTypedExpr> rightKeys;
  private final TypedExpr filter;
  private final RowType outputType;

  public AbstractJoinNode(
      String id,
      JoinType joinType,
      List<FieldAccessTypedExpr> leftKeys,
      List<FieldAccessTypedExpr> rightKeys,
      TypedExpr filter,
      PlanNode left,
      PlanNode right,
      RowType outputType) {
    super(id);
    this.sources = List.of(left, right);
    this.joinType = joinType;
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
    this.filter = filter;
    this.outputType = outputType;
  }

  @Override
  protected List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("joinType")
  public JoinType getJoinType() {
    return joinType;
  }

  @JsonGetter("leftKeys")
  public List<FieldAccessTypedExpr> getLeftKeys() {
    return leftKeys;
  }

  @JsonGetter("rightKeys")
  public List<FieldAccessTypedExpr> getRightKeys() {
    return rightKeys;
  }

  @JsonGetter("filter")
  public TypedExpr getFilter() {
    return filter;
  }

  @JsonGetter("outputType")
  public RowType getOutputType() {
    return outputType;
  }
}
