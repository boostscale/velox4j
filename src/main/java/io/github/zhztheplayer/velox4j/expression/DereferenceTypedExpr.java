package io.github.zhztheplayer.velox4j.expression;

import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public class DereferenceTypedExpr extends TypedExpr {
  protected DereferenceTypedExpr(Type returnType, List<TypedExpr> inputs) {
    super(returnType, inputs);
  }
}
