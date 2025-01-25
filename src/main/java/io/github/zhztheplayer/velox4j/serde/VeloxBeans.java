package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.type.TinyIntType;

public final class VeloxBeans {
  private VeloxBeans() {}

  public static void registerAll() {
    VeloxBeanRegistry.register("Velox4jQuery", Query.class);
    VeloxBeanRegistry.register("Type", TinyIntType.class);
  }
}
