package io.github.zhztheplayer.velox4j.aggregate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.sort.SortOrder;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public class Aggregate {
  public final CallTypedExpr call;
  public final List<Type> rawInputTypes;
  public final FieldAccessTypedExpr mask;
  public final List<FieldAccessTypedExpr> sortingKeys;
  public final List<SortOrder> sortingOrders;
  public final boolean distinct;

  @JsonCreator
  public Aggregate(@JsonProperty("call") CallTypedExpr call,
      @JsonProperty("rawInputTypes") List<Type> rawInputTypes,
      @JsonProperty("mask") FieldAccessTypedExpr mask,
      @JsonProperty("sortingKeys") List<FieldAccessTypedExpr> sortingKeys,
      @JsonProperty("sortingOrders") List<SortOrder> sortingOrders,
      @JsonProperty("distinct") boolean distinct) {
    this.call = call;
    this.rawInputTypes = rawInputTypes;
    this.mask = mask;
    this.sortingKeys = sortingKeys;
    this.sortingOrders = sortingOrders;
    this.distinct = distinct;
  }


  @JsonGetter("call")
  public CallTypedExpr getCall() {
    return call;
  }

  @JsonGetter("rawInputTypes")
  public List<Type> getRawInputTypes() {
    return rawInputTypes;
  }

  @JsonGetter("mask")
  public FieldAccessTypedExpr getMask() {
    return mask;
  }

  @JsonGetter("sortingKeys")
  public List<FieldAccessTypedExpr> getSortingKeys() {
    return sortingKeys;
  }

  @JsonGetter("sortingOrders")
  public List<SortOrder> getSortingOrders() {
    return sortingOrders;
  }

  @JsonGetter("distinct")
  public boolean isDistinct() {
    return distinct;
  }
}
