package io.github.zhztheplayer.velox4j.serializable;

import io.github.zhztheplayer.velox4j.jni.JniApi;

public class ISerializables {
  private final JniApi jniApi;

  public ISerializables(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public ISerializableCo asCpp(ISerializable iSerializable) {
    return jniApi.iSerializableAsCpp(iSerializable);
  }
}
