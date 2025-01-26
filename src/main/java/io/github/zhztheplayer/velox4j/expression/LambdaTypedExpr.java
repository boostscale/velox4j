package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.type.FunctionType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.Collections;

public class LambdaTypedExpr extends TypedExpr {
  private final Type signature;
  private final TypedExpr body;

  @JsonCreator
  private LambdaTypedExpr(@JsonProperty("type") Type returnType, @JsonProperty("signature") Type signature,
      @JsonProperty("body") TypedExpr body) {
    super(returnType, Collections.emptyList());
    this.signature = signature;
    this.body = body;
    Preconditions.checkArgument(returnType instanceof FunctionType,
        "LambdaTypedExpr returnType should be FunctionType");
  }

  public static LambdaTypedExpr create(RowType signature, TypedExpr body) {
    final FunctionType returnType = FunctionType.create(signature.getChildren(), body.getReturnType());
    return new LambdaTypedExpr(returnType, signature, body);
  }

  @JsonGetter("signature")
  public Type getSignature() {
    return signature;
  }

  @JsonGetter("body")
  public TypedExpr getBody() {
    return body;
  }
}
