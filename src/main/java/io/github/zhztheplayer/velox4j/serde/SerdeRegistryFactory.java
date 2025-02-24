package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SerdeRegistryFactory {
  private static final Map<Class<? extends NativeBean>, SerdeRegistryFactory> INSTANCES = new ConcurrentHashMap<>();

  public static SerdeRegistryFactory createForBaseClass(Class<? extends NativeBean> clazz) {
    return INSTANCES.compute(clazz, (k, v) -> {
      if (v != null) {
        throw new VeloxException("SerdeRegistryFactory already exists for " + k);
      }
      checkClass(k, INSTANCES.keySet());
      return new SerdeRegistryFactory(Collections.emptyList());
    });
  }

  private static void checkClass(Class<? extends NativeBean> clazz, Set<Class<? extends NativeBean>> others) {
    // The class to register should not be assignable from / to each other.
    for (Class<? extends NativeBean> other : others) {
      if (clazz.isAssignableFrom(other)) {
        throw new VeloxException(String.format("Class %s is not register-able because it is assignable from %s", clazz, other));
      }
      if (other.isAssignableFrom(clazz)) {
        throw new VeloxException(String.format("Class %s is not register-able because it is assignable to %s", clazz, other));
      }
    }
  }

  public static SerdeRegistryFactory getForBaseClass(Class<? extends NativeBean> clazz) {
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
