package io.github.zhztheplayer.velox4j.data;

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.Session;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public class BaseVectors {
  private BaseVectors() {
  }

  public static String serialize(List<? extends BaseVector> vectors) {
    return StaticJniApi.get().baseVectorSerialize(vectors);
  }

  public static String serialize(BaseVector vector) {
    return StaticJniApi.get().baseVectorSerialize(List.of(vector));
  }

  public static BaseVector deserialize(Session session, String serialized) {
    final List<BaseVector> vectors = session.baseVectorDeserialize(serialized);
    Preconditions.checkState(vectors.size() == 1,
        "Expected one vector, but got %s", vectors.size());
    return vectors.get(0);
  }

  public static Type getType(BaseVector vector) {
    return StaticJniApi.get().baseVectorGetType(vector);
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
}
