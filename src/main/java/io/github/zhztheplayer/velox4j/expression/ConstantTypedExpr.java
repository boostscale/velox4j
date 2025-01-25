package io.github.zhztheplayer.velox4j.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

import java.util.Collections;

public class ConstantTypedExpr extends TypedExpr {
  private final String serializedVector;

  @JsonCreator
  private ConstantTypedExpr(@JsonProperty("type") Type returnType,
      @JsonProperty("valueVector") String serializedVector) {
    super(returnType, Collections.emptyList());
    this.serializedVector = serializedVector;
  }

  public static ConstantTypedExpr create(JniApi jniApi, BufferAllocator alloc, FieldVector arrowVector) {
    final BaseVector baseVector = Arrow.fromArrowVector(jniApi, alloc, arrowVector);
    final Type type = jniApi.baseVectorGetType(baseVector);
    final String serialized = jniApi.baseVectorSerialize(baseVector);
    return new ConstantTypedExpr(type, serialized);
  }

  public static ConstantTypedExpr create(JniApi jniApi, String serialized) {
    final BaseVector baseVector = jniApi.baseVectorDeserialize(serialized);
    final Type type = jniApi.baseVectorGetType(baseVector);
    return new ConstantTypedExpr(type, serialized);
  }

  @JsonGetter("valueVector")
  public String getSerializedVector() {
    return serializedVector;
  }
}
