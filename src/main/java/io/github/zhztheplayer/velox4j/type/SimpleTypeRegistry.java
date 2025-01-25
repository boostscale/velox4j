package io.github.zhztheplayer.velox4j.type;

import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;

public class SimpleTypeRegistry extends SerdeRegistry<SimpleType> {
  private static final SimpleTypeRegistry INSTANCE = new SimpleTypeRegistry();

  private SimpleTypeRegistry() {
  }

  public static SimpleTypeRegistry get() {
    return INSTANCE;
  }
}
