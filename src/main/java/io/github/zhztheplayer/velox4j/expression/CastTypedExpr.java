package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.Collections;
import java.util.List;

public class CastTypedExpr extends TypedExpr {
  private final boolean nullOnFailure;

  @JsonCreator
  private CastTypedExpr(@JsonProperty("type") Type returnType,
      @JsonProperty("inputs") List<TypedExpr> inputs,
      @JsonProperty("nullOnFailure") boolean nullOnFailure) {
    super(returnType, inputs);
    Preconditions.checkArgument(inputs.size() == 1,
        "CastTypedExpr should have 1 input, but has %s", inputs.size());
    this.nullOnFailure = nullOnFailure;
  }

  public static CastTypedExpr create(Type returnType, TypedExpr input, boolean nullOnFailure) {
    return new CastTypedExpr(returnType, Collections.singletonList(input), nullOnFailure);
  }

  @JsonGetter("nullOnFailure")
  public boolean isNullOnFailure() {
    return nullOnFailure;
  }
}
