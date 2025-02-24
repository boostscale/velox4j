package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public class CallTypedExpr extends TypedExpr {
  private final String functionName;

  @JsonCreator
  public CallTypedExpr(
      @JsonProperty("type") Type returnType,
      @JsonProperty("inputs") List<TypedExpr> inputs,
      @JsonProperty("functionName") String functionName
  ) {
    super(returnType, inputs);
    this.functionName = functionName;
  }

  @JsonGetter("functionName")
  public String getFunctionName() {
    return functionName;
  }
}
