package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.sort.SortOrder;

import java.util.List;

public class OrderByNode extends PlanNode {
  private final List<PlanNode> sources;
  private final List<FieldAccessTypedExpr> sortingKeys;
  private final List<SortOrder> sortingOrders;
  private final boolean partial;

  @JsonCreator
  public OrderByNode(
      @JsonProperty("id") String id,
      @JsonProperty("sources") List<PlanNode> sources,
      @JsonProperty("sortingKeys") List<FieldAccessTypedExpr> sortingKeys,
      @JsonProperty("sortingOrders") List<SortOrder> sortingOrders,
      @JsonProperty("partial") boolean partial) {
    super(id);
    this.sources = sources;
    this.sortingKeys = sortingKeys;
    this.sortingOrders = sortingOrders;
    this.partial = partial;
  }

  @Override
  protected List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("sortingKeys")
  public List<FieldAccessTypedExpr> sortingKeys() {
    return sortingKeys;
  }

  @JsonGetter("sortingOrders")
  public List<SortOrder> sortingOrders() {
    return sortingOrders;
  }

  @JsonGetter("partial")
  public boolean isPartial() {
    return partial;
  }
}
