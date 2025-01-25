package io.github.zhztheplayer.velox4j.bean;

import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.type.Types;

public final class VeloxBeans {
  private VeloxBeans() {
  }

  public static void registerAll() {
    Types.registerAll();
    VeloxBeanRegistry.get().register("Velox4jQuery", Query.class);
  }
}
