package io.github.zhztheplayer.velox4j.variant;

import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistryFactory;

public class Variants {
  private static final SerdeRegistry TYPE_REGISTRY = SerdeRegistryFactory
      .createForBaseClass(Variant.class).key("type");

  private Variants() {

  }

  public static void registerAll() {
    Serde.registerBaseClass(Variant.class);
    TYPE_REGISTRY.registerClass("BOOLEAN", BooleanValue.class);
    TYPE_REGISTRY.registerClass("TINYINT", TinyIntValue.class);
    TYPE_REGISTRY.registerClass("SMALLINT", SmallIntValue.class);
    TYPE_REGISTRY.registerClass("INTEGER", IntegerValue.class);
    TYPE_REGISTRY.registerClass("BIGINT", BigIntValue.class);
    TYPE_REGISTRY.registerClass("HUGEINT", HugeIntValue.class);
    TYPE_REGISTRY.registerClass("REAL", RealValue.class);
    TYPE_REGISTRY.registerClass("DOUBLE", DoubleValue.class);
    TYPE_REGISTRY.registerClass("VARCHAR", VarCharValue.class);
    TYPE_REGISTRY.registerClass("VARBINARY", VarBinaryValue.class);
  }
}
