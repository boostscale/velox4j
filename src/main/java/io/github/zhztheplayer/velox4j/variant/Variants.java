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
    ROOT_REGISTRY.registerClass("BOOLEAN", BooleanValue.class);
    ROOT_REGISTRY.registerClass("TINYINT", TinyIntValue.class);
    ROOT_REGISTRY.registerClass("SMALLINT", SmallIntValue.class);
    ROOT_REGISTRY.registerClass("INTEGER", IntegerValue.class);
    ROOT_REGISTRY.registerClass("BIGINT", BigIntValue.class);
    ROOT_REGISTRY.registerClass("HUGEINT", HugeIntValue.class);
    ROOT_REGISTRY.registerClass("REAL", RealValue.class);
    ROOT_REGISTRY.registerClass("DOUBLE", DoubleValue.class);
  }
}
