package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.jni.CalledFromNative;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.CppObject;

import java.util.Iterator;

public class DownIterator {
  private final Iterator<RowVector> delegated;

  public DownIterator(Iterator<RowVector> delegated) {
    this.delegated = delegated;
  }

  @CalledFromNative
  public boolean hasNext() {
    return delegated.hasNext();
  }

  @CalledFromNative
  public long next() {
    return delegated.next().id();
  }
}
