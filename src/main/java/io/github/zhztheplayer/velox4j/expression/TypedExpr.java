package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import io.github.zhztheplayer.velox4j.bean.VeloxBean;
import io.github.zhztheplayer.velox4j.type.Type;

public abstract class TypedExpr extends VeloxBean {
  private final Type returnType;

  @JsonCreator
  protected TypedExpr(Type returnType) {
    this.returnType = returnType;
  }

  @JsonGetter("type")
  public Type getReturnType() {
    return returnType;
  }
}
