package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.memory.AllocationListener;

public class StaticJniWrapper {
  private static final StaticJniWrapper INSTANCE = new StaticJniWrapper();

  static StaticJniWrapper get() {
    return INSTANCE;
  }

  private StaticJniWrapper() {}

  // Global initialization.
  native void initialize(String globalConfJson);

  // Memory.
  native long createMemoryManager(AllocationListener listener);

  // Lifecycle.
  native long createSession(long memoryManagerId);
  native void releaseCppObject(long objectId);
}
