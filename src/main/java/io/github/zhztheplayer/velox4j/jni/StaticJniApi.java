package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;

public class StaticJniApi {
  private static final StaticJniApi INSTANCE = new StaticJniApi();

  public static StaticJniApi get() {
    return INSTANCE;
  }

  private final StaticJniWrapper jni = StaticJniWrapper.get();

  private StaticJniApi() {}

  public MemoryManager createMemoryManager(AllocationListener listener) {
    return new MemoryManager(jni.createMemoryManager(listener));
  }

  public Session createSession(MemoryManager memoryManager) {
    return LocalSession.create(jni.createSession(memoryManager.id()));
  }

  public void releaseCppObject(CppObject obj) {
    jni.releaseCppObject(obj.id());
  }
}
