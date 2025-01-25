package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.lifecycle.CppObject;

public class BaseVector implements CppObject {
  private final JniApi jniApi;
  private final long id;

  public BaseVector(JniApi jniApi, long id) {
    this.jniApi = jniApi;
    this.id = id;
  }

  @Override
  public JniApi jniApi() {
    return jniApi;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public void close() {
    jniApi.releaseCppObject(this);
  }
}
