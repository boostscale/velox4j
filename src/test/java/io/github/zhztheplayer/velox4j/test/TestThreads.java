package io.github.zhztheplayer.velox4j.test;

import com.google.common.util.concurrent.UncaughtExceptionHandlers;

public final class TestThreads {
  public static Thread newTestThread(Runnable target) {
    final Thread t = new Thread(target);
    t.setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit());
    return t;
  }
}
