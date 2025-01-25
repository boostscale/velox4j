package io.github.zhztheplayer.velox4j.expression;

import io.github.zhztheplayer.velox4j.type.Type;

public class CallTypedExpr extends TypedExpr {
  protected CallTypedExpr(Type returnType) {
    super(returnType);
  }
}
