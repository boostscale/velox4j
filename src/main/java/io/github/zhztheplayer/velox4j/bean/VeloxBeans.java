package io.github.zhztheplayer.velox4j.bean;

import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;
import io.github.zhztheplayer.velox4j.serde.SerdeRegistryFactory;
import io.github.zhztheplayer.velox4j.type.TinyIntType;

public final class VeloxBeans {
  private VeloxBeans() {

  }

  public static void registerAll() {
    final SerdeRegistry typeRegistry = SerdeRegistryFactory.get().key("name")
        .registerFactory("Type")
        .key("type");

    typeRegistry.registerClass("TINYINT", TinyIntType.class);
  }
}
