package io.github.zhztheplayer.velox4j.jni;

public interface CppObject extends AutoCloseable {
  long id();

  @Override
  default void close() {
    JniApi.staticInstance().releaseCppObject(this);
  };
}
