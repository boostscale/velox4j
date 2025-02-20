package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;

public class SelectivityVectors {
  private final JniApi jniApi;

  public SelectivityVectors(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public SelectivityVector create(int length) {
    return jniApi.createSelectivityVector(length);
  }

  public static boolean isValid(SelectivityVector vector, int idx) {
    return StaticJniApi.get().selectivityVectorIsValid(vector, idx);
  }
}
