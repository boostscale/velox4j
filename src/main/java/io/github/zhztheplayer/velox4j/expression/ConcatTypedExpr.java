package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.Collections;
import java.util.List;

public class ConcatTypedExpr extends TypedExpr {
  @JsonCreator
  public ConcatTypedExpr(@JsonProperty("type") Type returnType,
      @JsonProperty("inputs") List<TypedExpr> inputs) {
    super(returnType, inputs);
  }
}
