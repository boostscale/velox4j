package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.Collections;
import java.util.List;

public class FieldAccessTypedExpr extends TypedExpr {
  private final String fieldName;

  @JsonCreator
  private FieldAccessTypedExpr(@JsonProperty("type") Type returnType,
      @JsonProperty("inputs") List<TypedExpr> inputs,
      @JsonProperty("fieldName") String fieldName) {
    super(returnType, inputs);
    this.fieldName = fieldName;
    Preconditions.checkArgument(inputs.size() <= 1,
        "FieldAccessTypedExpr should have 0 or 1 input, but has %s", inputs.size());
  }

  public static FieldAccessTypedExpr create(Type returnType, String fieldName) {
    return new FieldAccessTypedExpr(returnType, Collections.emptyList(), fieldName);
  }

  public static FieldAccessTypedExpr create(TypedExpr input, String fieldName) {
    final Type inputType = input.getReturnType();
    Preconditions.checkArgument(inputType instanceof RowType,
        "FieldAccessTypedExpr input should be RowType");
    final Type returnType = ((RowType) inputType).findChild(fieldName);
    return new FieldAccessTypedExpr(returnType, Collections.singletonList(input), fieldName);
  }

  @JsonGetter("fieldName")
  public String getFieldName() {
    return fieldName;
  }
}
