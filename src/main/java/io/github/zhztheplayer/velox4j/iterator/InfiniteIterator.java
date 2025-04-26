package io.github.zhztheplayer.velox4j.iterator;

public interface InfiniteIterator<T> extends AutoCloseable {
  boolean available();

  void waitFor();

  T get();
}
