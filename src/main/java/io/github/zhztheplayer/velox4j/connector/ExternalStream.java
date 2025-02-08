package io.github.zhztheplayer.velox4j.connector;

import io.github.zhztheplayer.velox4j.jni.CppObject;

public class ExternalStream implements CppObject {
  private final long id;

  public ExternalStream(long id) {
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }
}
