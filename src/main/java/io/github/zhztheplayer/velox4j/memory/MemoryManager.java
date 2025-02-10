package io.github.zhztheplayer.velox4j.memory;

import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;

public class MemoryManager implements CppObject {
  public static MemoryManager create(AllocationListener listener) {
    return StaticJniApi.get().createMemoryManager(listener);
  }

  private final long id;

  public MemoryManager(long id) {
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }
}
