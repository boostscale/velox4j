package io.github.zhztheplayer.velox4j.memory;

import io.github.zhztheplayer.velox4j.jni.CalledFromNative;

public interface AllocationListener {
  AllocationListener NOOP = new NoopAllocationListener();

  @CalledFromNative
  void allocationChanged(long diff);
}
