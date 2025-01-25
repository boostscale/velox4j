package io.github.zhztheplayer.velox4j.serde;

import java.util.*;

public class SerdeRegistryFactory {
  private static final SerdeRegistryFactory INSTANCE = new SerdeRegistryFactory(Collections.emptyList());

  public static SerdeRegistryFactory get() {
    return INSTANCE;
  }

  private final Map<String, SerdeRegistry> registries = new HashMap<>();
  private final List<SerdeRegistry.KvPair> kvs;

  SerdeRegistryFactory(List<SerdeRegistry.KvPair> kvs) {
    this.kvs = kvs;
  }

  public SerdeRegistry key(String key) {
    synchronized (this) {
      if (!registries.containsKey(key)) {
        registries.put(key, new SerdeRegistry(kvs, key));
      }
      return registries.get(key);
    }
  }

  public Set<String> keys() {
    synchronized (this) {
      return registries.keySet();
    }
  }
}
