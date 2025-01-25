package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.zhztheplayer.velox4j.bean.VeloxBean;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public abstract class TypedExpr extends VeloxBean {
  private final Type returnType;
  private final List<TypedExpr> inputs;

  protected TypedExpr(Type returnType, List<TypedExpr> inputs) {
    this.returnType = returnType;
    this.inputs = inputs;
  }

  @JsonGetter("type")
  public Type getReturnType() {
    return returnType;
  }

  @JsonGetter("inputs")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<TypedExpr> getInputs() {
    return inputs;
  }
}
