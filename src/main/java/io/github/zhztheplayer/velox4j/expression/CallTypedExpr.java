package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.Collections;
import java.util.List;

public class CallTypedExpr extends TypedExpr {
  private final String functionName;
  private final List<TypedExpr> inputs;

  @JsonCreator
  public CallTypedExpr(@JsonProperty("type") Type returnType,
      @JsonProperty("inputs") List<TypedExpr> inputs, @JsonProperty("functionName") String functionName) {
    super(returnType);
    this.inputs = inputs == null ? Collections.emptyList() : Collections.unmodifiableList(inputs);
    this.functionName = functionName;
  }

  @JsonGetter("functionName")
  public String getFunctionName() {
    return functionName;
  }

  @JsonGetter("inputs")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<TypedExpr> getInputs() {
    return inputs;
  }
}
