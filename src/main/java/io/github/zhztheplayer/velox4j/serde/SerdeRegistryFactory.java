package io.github.zhztheplayer.velox4j.serde;

import java.util.HashMap;
import java.util.Map;

public class SerdeRegistryFactory {
  private static final SerdeRegistryFactory INSTANCE = new SerdeRegistryFactory("ROOT");

  public static SerdeRegistryFactory get() {
    return INSTANCE;
  }

  private final Map<String, SerdeRegistry> registries = new HashMap<>();
  private final String prefix;

  SerdeRegistryFactory(String prefix) {
    this.prefix = prefix;
  }

  public SerdeRegistry key(String key) {
    synchronized (this) {
      if (!registries.containsKey(key)) {
        registries.put(key, new SerdeRegistry(String.format("%s.%s", prefix, key)));
      }
      return registries.get(key);
    }
  }
}
