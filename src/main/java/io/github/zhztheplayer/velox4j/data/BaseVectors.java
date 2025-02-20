package io.github.zhztheplayer.velox4j.data;

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;
import io.github.zhztheplayer.velox4j.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Table;

import java.util.List;

public class BaseVectors {
  private final JniApi jniApi;

  public BaseVectors(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public static String serialize(List<? extends BaseVector> vectors) {
    return StaticJniApi.get().baseVectorSerialize(vectors);
  }

  public static String serialize(BaseVector vector) {
    return StaticJniApi.get().baseVectorSerialize(List.of(vector));
  }

  public BaseVector deserialize(String serialized) {
    final List<BaseVector> vectors = jniApi.baseVectorDeserialize(serialized);
    Preconditions.checkState(vectors.size() == 1,
        "Expected one vector, but got %s", vectors.size());
    return vectors.get(0);
  }

  public static Type getType(BaseVector vector) {
    return StaticJniApi.get().baseVectorGetType(vector);
  }

  public static int getSize(BaseVector vector) {
    return StaticJniApi.get().baseVectorGetSize(vector);
  }

  public static BaseVector wrapInConstant(BaseVector vector, int length, int index) {
    return vector.jniApi().baseVectorWrapInConstant(vector, length, index);
  }

  public static VectorEncoding getEncoding(BaseVector vector) {
    return StaticJniApi.get().baseVectorGetEncoding(vector);
  }

  public static RowVector asRowVector(BaseVector vector) {
    return vector.jniApi().baseVectorAsRowVector(vector);
  }

  public static String toString(BufferAllocator alloc, BaseVector vector) {
    try (final FieldVector fv = Arrow.toArrowVector(alloc, vector)) {
      return fv.toString();
    }
  }
}
