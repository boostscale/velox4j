package io.github.zhztheplayer.velox4j.lifecycle;

import io.github.zhztheplayer.velox4j.jni.JniApi;

public interface CppObject extends AutoCloseable {
  JniApi jniApi();

  long id();

  @Override
  default void close() {
    jniApi().releaseCppObject(this);
  };
}
