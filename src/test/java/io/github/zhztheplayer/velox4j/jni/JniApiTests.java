package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.memory.MemoryManager;

public final class JniApiTests {
  private JniApiTests() {

  }

  public static LocalSession createLocalSession(MemoryManager memoryManager) {
    return StaticJniApi.get().createSession(memoryManager);
  }
}
