package io.github.zhztheplayer.velox4j.lifecycle;

public interface CppObject extends AutoCloseable {
  long address();
}
