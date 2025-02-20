package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.JniApi;

public class SelectivityVector implements CppObject {
  private final long id;

  public SelectivityVector(long id) {
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }
}
