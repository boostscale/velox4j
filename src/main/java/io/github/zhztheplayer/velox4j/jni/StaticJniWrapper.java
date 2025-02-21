package io.github.zhztheplayer.velox4j.jni;

import com.google.common.annotations.VisibleForTesting;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;

public class StaticJniWrapper {
  private static final StaticJniWrapper INSTANCE = new StaticJniWrapper();

  static StaticJniWrapper get() {
    return INSTANCE;
  }

  private StaticJniWrapper() {}

  // Global initialization.
  native void initialize(String globalConfJson);

  // Memory.
  native long createMemoryManager(AllocationListener listener);

  // Lifecycle.
  native long createSession(long memoryManagerId);
  native void releaseCppObject(long objectId);

  // For UpIterator.
  native boolean upIteratorHasNext(long address);

  // For Variant.
  native String variantInferType(String json);

  // For BaseVector / RowVector / SelectivityVector.
  native void baseVectorToArrow(long rvAddress, long cSchema, long cArray);
  native String baseVectorSerialize(long[] id);
  native String baseVectorGetType(long id);
  native int baseVectorGetSize(long id);
  native String baseVectorGetEncoding(long id);
  native void baseVectorAppend(long id, long toAppendId);
  native boolean selectivityVectorIsValid(long id, int idx);

  // For test.
  @VisibleForTesting
  native String deserializeAndSerializeVariant(String json);
}
