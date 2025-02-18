package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.Velox4j;

import java.util.concurrent.atomic.AtomicBoolean;

public class Velox4jTests {
  private static final AtomicBoolean initialized = new AtomicBoolean(false);

  public static void ensureInitialized() {
    if (!initialized.compareAndSet(false, true)) {
      return;
    }
    Velox4j.initialize();
  }
}
