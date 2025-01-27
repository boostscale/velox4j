package io.github.zhztheplayer.velox4j.bean;

import io.github.zhztheplayer.velox4j.serde.NativeBean;

/**
 * Java binding of Velox's ISerializable API. A VeloxSerializable can be serialized to JSON and
 * deserialized from JSON.
 */
public abstract class VeloxSerializable implements NativeBean {
  protected VeloxSerializable() {
  }
}
