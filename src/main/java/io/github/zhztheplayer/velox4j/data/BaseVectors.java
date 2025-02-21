package io.github.zhztheplayer.velox4j.data;

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public class BaseVectors {
  private final JniApi jniApi;

  public BaseVectors(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public BaseVector createEmpty(Type type) {
    return jniApi.createEmptyBaseVector(type);
  }

  public static String serializeOne(BaseVector vector) {
    return StaticJniApi.get().baseVectorSerialize(List.of(vector));
  }

  public BaseVector deserializeOne(String serialized) {
    final List<BaseVector> vectors = jniApi.baseVectorDeserialize(serialized);
    Preconditions.checkState(vectors.size() == 1,
        "Expected one vector, but got %s", vectors.size());
    return vectors.get(0);
  }

  public static String serializeAll(List<? extends BaseVector> vectors) {
    return StaticJniApi.get().baseVectorSerialize(vectors);
  }

  public List<BaseVector> deserializeAll(String serialized) {
    return jniApi.baseVectorDeserialize(serialized);
  }
}
