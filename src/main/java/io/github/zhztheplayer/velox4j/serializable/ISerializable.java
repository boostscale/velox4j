package io.github.zhztheplayer.velox4j.serializable;

import io.github.zhztheplayer.velox4j.serde.NativeBean;

/**
 * Java binding of Velox's ISerializable API. A ISerializable can be serialized to JSON and
 * deserialized from JSON.
 */
public abstract class ISerializable implements NativeBean {
  protected ISerializable() {
  }
}
