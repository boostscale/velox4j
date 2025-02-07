package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.JniApi;

public class ExternalStream implements CppObject {
  private final JniApi jniApi;
  private final long id;

  public ExternalStream(JniApi jniApi, long id) {
    this.jniApi = jniApi;
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }
}
