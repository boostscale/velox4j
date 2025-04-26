package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.jni.CalledFromNative;

public interface DownIterator {
  enum State {
    AVAILABLE(0),
    BLOCKED(1),
    FINISHED(2);

    private final int id;

    State(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }
  }

  @CalledFromNative
  int advance();

  @CalledFromNative
  void waitFor() throws InterruptedException;

  @CalledFromNative
  long get();

  @CalledFromNative
  void close();
}
