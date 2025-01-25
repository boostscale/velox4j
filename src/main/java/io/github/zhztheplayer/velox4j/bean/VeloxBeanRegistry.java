package io.github.zhztheplayer.velox4j.bean;

import io.github.zhztheplayer.velox4j.serde.SerdeRegistry;

public class VeloxBeanRegistry extends SerdeRegistry<VeloxBean> {
  private static final VeloxBeanRegistry INSTANCE = new VeloxBeanRegistry();
  private VeloxBeanRegistry() {
  }
  public static VeloxBeanRegistry get() {
    return INSTANCE;
  }
}
