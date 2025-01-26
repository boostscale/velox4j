package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ConcatTypedExpr extends TypedExpr {
  @JsonCreator
  private ConcatTypedExpr(@JsonProperty("type") Type returnType,
      @JsonProperty("inputs") List<TypedExpr> inputs) {
    super(returnType, inputs);
    Preconditions.checkArgument(returnType instanceof RowType,
        "ConcatTypedExpr returnType should be RowType");
  }

  public static ConcatTypedExpr create(List<String> names, List<TypedExpr> inputs) {
    Preconditions.checkArgument(names.size() == inputs.size(),
        "ConcatTypedExpr should have same number of names and inputs");
    return new ConcatTypedExpr(new RowType(names,
        inputs.stream().map(TypedExpr::getReturnType).collect(Collectors.toList())), inputs);
  }
}
