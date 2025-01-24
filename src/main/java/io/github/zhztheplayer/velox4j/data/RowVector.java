package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.lifecycle.CppObject;

public class RowVector implements CppObject {
  private final long address;

  public RowVector(long address) {
    this.address = address;
  }

  @Override
  public long address() {
    return address;
  }

  @Override
  public void close() {
    JniApi.closeCppObject(this);
  }
}
