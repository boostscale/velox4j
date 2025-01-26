package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.aggregate.AggregateStep;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public class AggregationNode extends PlanNode {
  private final AggregateStep step;
  private final List<FieldAccessTypedExpr> groupingKeys;
  private final List<FieldAccessTypedExpr> preGroupedKeys;
  private final List<String> aggregateNames;
  private final List<Aggregate> aggregates;
  private final boolean ignoreNullKeys;

  private final List<PlanNode> sources;

  private final FieldAccessTypedExpr groupId;
  private final List<Integer> globalGroupingSets;

  @JsonCreator
  public AggregationNode(
      @JsonProperty("id") String id,
      @JsonProperty("step") AggregateStep step,
      @JsonProperty("groupingKeys") List<FieldAccessTypedExpr> groupingKeys,
      @JsonProperty("preGroupedKeys") List<FieldAccessTypedExpr> preGroupedKeys,
      @JsonProperty("aggregateNames") List<String> aggregateNames,
      @JsonProperty("aggregates") List<Aggregate> aggregates,
      @JsonProperty("ignoreNullKeys") boolean ignoreNullKeys,
      @JsonProperty("sources") List<PlanNode> sources,
      @JsonProperty("groupId") FieldAccessTypedExpr groupId,
      @JsonProperty("globalGroupingSets") List<Integer> globalGroupingSets) {
    super(id);
    this.step = step;
    this.groupingKeys = groupingKeys;
    this.preGroupedKeys = preGroupedKeys;
    this.aggregateNames = aggregateNames;
    this.aggregates = aggregates;
    this.ignoreNullKeys = ignoreNullKeys;
    this.sources = sources;
    this.groupId = groupId;
    this.globalGroupingSets = globalGroupingSets;
  }

  @Override
  public List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("step")
  public AggregateStep getStep() {
    return step;
  }

  @JsonGetter("groupingKeys")
  public List<FieldAccessTypedExpr> getGroupingKeys() {
    return groupingKeys;
  }

  @JsonGetter("preGroupedKeys")
  public List<FieldAccessTypedExpr> getPreGroupedKeys() {
    return preGroupedKeys;
  }

  @JsonGetter("aggregateNames")
  public List<String> getAggregateNames() {
    return aggregateNames;
  }

  @JsonGetter("aggregates")
  public List<Aggregate> getAggregates() {
    return aggregates;
  }

  @JsonGetter("ignoreNullKeys")
  public boolean isIgnoreNullKeys() {
    return ignoreNullKeys;
  }

  @JsonGetter("groupId")
  public FieldAccessTypedExpr getGroupId() {
    return groupId;
  }

  @JsonGetter("globalGroupingSets")
  public List<Integer> getGlobalGroupingSets() {
    return globalGroupingSets;
  }
}
