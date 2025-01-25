package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.jni.JniApi;

public class BaseVectors {
  private BaseVectors() {
  }

  public static String serialize(BaseVector vector) {
    return vector.jniApi().baseVectorSerialize(vector);
  }

  public static String deserialize(JniApi jniApi, BaseVector vector) {
    return jniApi.baseVectorSerialize(vector);
  }
}
