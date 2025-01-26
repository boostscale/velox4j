package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.Collections;

public class InputTypedExpr extends TypedExpr {
  @JsonCreator
  public InputTypedExpr(@JsonProperty("type") Type returnType) {
    super(returnType, Collections.emptyList());
  }
}
