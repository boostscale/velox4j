package io.github.zhztheplayer.velox4j.type;

import io.github.zhztheplayer.velox4j.bean.VeloxBeanRegistry;

public final class Types {
  private Types() {

  }

  public static void registerAll() {
    registerSimpleType("TINYINT", TinyIntType.class);
  }

  private static void registerSimpleType(String typeKey, Class<? extends SimpleType> clazz) {
    VeloxBeanRegistry.get().register("Type", clazz);
  }
}
