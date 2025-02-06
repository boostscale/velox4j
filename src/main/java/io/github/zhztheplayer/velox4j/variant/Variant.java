package io.github.zhztheplayer.velox4j.variant;

import io.github.zhztheplayer.velox4j.serde.NativeBean;

/**
 * Java binding of Velox's variant API. A Variant can be serialized to JSON and
 * deserialized from JSON.
 */
public abstract class Variant implements NativeBean {
  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract int hashCode();

  @Override
  public abstract String toString();
}
