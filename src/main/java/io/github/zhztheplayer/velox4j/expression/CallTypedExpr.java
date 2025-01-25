package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public class CallTypedExpr extends TypedExpr {
  private final String functionName;
  private final List<TypedExpr> inputs;

  @JsonCreator
  public CallTypedExpr(Type returnType, @JsonProperty("functionName") String functionName,
      @JsonProperty("inputs") List<TypedExpr> inputs) {
    super(returnType);
    Preconditions.checkArgument(inputs != null);
    this.functionName = functionName;
    this.inputs = inputs;
  }

  @JsonGetter("functionName")
  public String getFunctionName() {
    return functionName;
  }

  @JsonGetter("inputs")
  public List<TypedExpr> getInputs() {
    return inputs;
  }
}
