package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SerdeRegistryFactory {
  private static final Map<Class<?>, SerdeRegistryFactory> INSTANCES = new ConcurrentHashMap<>();

  public static SerdeRegistryFactory createForBaseClass(Class<?> clazz) {
    return INSTANCES.compute(clazz, (k, v) -> {
      if (v != null) {
        throw new VeloxException("SerdeRegistryFactory already exists for " + clazz);
      }
      return new SerdeRegistryFactory(Collections.emptyList());
    });
  }

  public static SerdeRegistryFactory getForBaseClass(Class<?> clazz) {
    return INSTANCES.get(clazz);
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
