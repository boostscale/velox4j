package io.github.zhztheplayer.velox4j.memory;

import io.github.zhztheplayer.velox4j.jni.CppObject;

public class MemoryManager implements CppObject {

  private final long id;

  public MemoryManager(long id) {
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }
}
