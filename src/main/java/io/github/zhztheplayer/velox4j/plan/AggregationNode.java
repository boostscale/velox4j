package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;

import java.util.List;

public class AggregationNode extends PlanNode {
  private final AggregateStep step;
  private final List<FieldAccessTypedExpr> groupingKeys;
  private final List<FieldAccessTypedExpr> preGroupedKeys;
  private final List<String> aggregateNames_;
  private final List<Aggregate> aggregates_;
  private final boolean ignoreNullKeys_;

  @JsonCreator
  protected AggregationNode(@JsonProperty("id") String id) {
    super(id);
  }
}
