package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.BaseVectors;
import io.github.zhztheplayer.velox4j.data.VectorEncoding;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.Variant;

import java.util.Collections;

public abstract class ConstantTypedExpr extends TypedExpr {

  private ConstantTypedExpr(Type returnType) {
    super(returnType, Collections.emptyList());
  }

  public static ConstantTypedExpr create(BaseVector vector) {
    final BaseVector constVector;
    if (BaseVectors.getEncoding(vector) == VectorEncoding.CONSTANT) {
      constVector = vector;
    } else {
      constVector = BaseVectors.wrapInConstant(vector, 1, 0);
    }
    final String serialized = BaseVectors.serialize(constVector);
    final Type type = BaseVectors.getType(vector);
    return new WithVector(type, serialized);
  }

  public static class WithVector extends ConstantTypedExpr {
    private final String serializedVector;

    @JsonCreator
    private WithVector(@JsonProperty("type") Type returnType,
        @JsonProperty("valueVector") String serializedVector) {
      super(returnType);
      this.serializedVector = serializedVector;
    }

    @JsonGetter("valueVector")
    public String getSerializedVector() {
      return serializedVector;
    }
  }

  public static class WithValue extends ConstantTypedExpr {
    private final Variant value;

    private WithValue(@JsonProperty("type") Type returnType,
        @JsonProperty("value") Variant value) {
      super(returnType);
      this.value = value;
    }

    @JsonGetter("value")
    public Variant getValue() {
      return value;
    }
  }
}
