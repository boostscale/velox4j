package io.github.zhztheplayer.velox4j.serde;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class VeloxBeanRegistry {
  private static final Map<String, Class<? extends VeloxBean>> registry = new HashMap<>();

  public static void register(String key, Class<? extends VeloxBean> bean) {
    synchronized (registry) {
      Preconditions.checkArgument(!registry.containsKey(key),
          "Velox bean type already registered for key: %s", key);
    }
    registry.put(key, bean);
  }

  public static Class<? extends VeloxBean> find(String key) {
    Preconditions.checkArgument(registry.containsKey(key),
        "Velox bean type not registered for key: %s", key);
    return registry.get(key);
  }
}
