package io.github.zhztheplayer.velox4j;

import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.jni.JniWorkspace;
import io.github.zhztheplayer.velox4j.serializable.VeloxSerializables;
import io.github.zhztheplayer.velox4j.variant.Variants;

import java.util.concurrent.atomic.AtomicBoolean;

public class Velox4j {
  private static final AtomicBoolean initialized = new AtomicBoolean(false);

  public static void initialize() {
    if (!initialized.compareAndSet(false, true)) {
      throw new VeloxException("Velox4J has already been initialized");
    }
    initialize0();
  }

  public static void ensureInitialized() {
    if (!initialized.compareAndSet(false, true)) {
      return;
    }
    initialize0();
  }

  private static void initialize0() {
    JniWorkspace.getDefault().libLoader().load("lib/libvelox4j.so");
    Variants.registerAll();
    VeloxSerializables.registerAll();
  }
}
