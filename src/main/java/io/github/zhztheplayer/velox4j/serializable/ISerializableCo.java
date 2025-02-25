package io.github.zhztheplayer.velox4j.serializable;

import io.github.zhztheplayer.velox4j.jni.CppObject;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;

/**
 * Binds a CPP ISerializable object.
 */
public class ISerializableCo implements CppObject {
  private final long id;

  public ISerializableCo(long id) {
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  public ISerializable asJava() {
    return StaticJniApi.get().iSerializableAsJava(this);
  }
}
