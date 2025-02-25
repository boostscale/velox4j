package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonGetter;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.Collections;
import java.util.List;

public abstract class TypedExpr extends ISerializable {
  private final Type returnType;
  private final List<TypedExpr> inputs;

  protected TypedExpr(Type returnType, List<TypedExpr> inputs) {
    this.returnType = returnType;
    this.inputs = inputs == null ? Collections.emptyList() : Collections.unmodifiableList(inputs);
  }

  @JsonGetter("type")
  public Type getReturnType() {
    return returnType;
  }

  @JsonGetter("inputs")
  public List<TypedExpr> getInputs() {
    return inputs;
  }
}
