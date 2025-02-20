package io.github.zhztheplayer.velox4j.jni;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.data.BaseVectors;
import io.github.zhztheplayer.velox4j.data.RowVectors;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.query.Query;

public interface Session extends CppObject {
  static Session create(MemoryManager memoryManager) {
    return StaticJniApi.get().createSession(memoryManager);
  }

  UpIterator executeQuery(Query query);

  ExternalStream newExternalStream(DownIterator itr);

  BaseVectors baseVectorOps();

  RowVectors rowVectorOps();

  Arrow arrowOps();
}
