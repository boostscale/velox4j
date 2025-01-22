package io.github.zhztheplayer.velox4j;

import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.jni.JniWorkspace;

import java.util.concurrent.atomic.AtomicBoolean;

public class Velox4J {
  private static final AtomicBoolean initialized = new AtomicBoolean(false);

  public static void initialize() {
    if (!initialized.compareAndSet(false, true)) {
      throw new VeloxException("Velox4J has already been initialized");
    }
    JniWorkspace.getDefault().libLoader().load("lib/libvelox4j.so");
  }
}
