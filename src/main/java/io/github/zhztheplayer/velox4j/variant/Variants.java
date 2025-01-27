package io.github.zhztheplayer.velox4j.variant;

import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistryFactory;

public class Variants {
  private static final SerdeRegistry ROOT_REGISTRY = SerdeRegistryFactory
      .createForBaseClass(Variant.class).key("type");

  private Variants() {

  }

  public static void registerAll() {
    Serde.registerBaseClass(Variant.class);
  }
}
