package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.util.HashMap;
import java.util.Map;

public class SerdeRegistry {
  private final String prefix;

  private final Map<String, SerdeRegistryFactory> subFactories = new HashMap<>();
  private final Map<String, Class<?>> classes = new HashMap<>();

  public SerdeRegistry(String prefix) {
    this.prefix = prefix;
  }

  public SerdeRegistryFactory factory(String value) {
    synchronized (this) {
      checkDup(value);
      final SerdeRegistryFactory factory = new SerdeRegistryFactory(String.format("%s.%s", prefix, value));
      subFactories.put(value, factory);
      return factory;
    }
  }

  public void register(String value, Class<?> clazz) {
    synchronized (this) {
      checkDup(value);
      classes.put(value, clazz);
    }
  }

  public boolean contains(String value) {
    synchronized (this) {
      return isFactory(value) || isClass(value);
    }
  }

  public boolean isFactory(String value) {
    synchronized (this) {
      return subFactories.containsKey(value);
    }
  }

  public boolean isClass(String value) {
    synchronized (this) {
      return classes.containsKey(value);
    }
  }

  public SerdeRegistryFactory getFactory(String value) {
    synchronized (this) {
      if (!subFactories.containsKey(value)) {
        throw new VeloxException(String.format("Value %s.%s is not added as a sub-factory: ", prefix, value));
      }
      return subFactories.get(value);
    }
  }

  public Class<?> getClass(String value) {
    synchronized (this) {
      if (!classes.containsKey(value)) {
        throw new VeloxException(String.format("Value %s.%s is not added as a class: ", prefix, value));
      }
      return classes.get(value);
    }
  }

  private void checkDup(String value) {
    if (subFactories.containsKey(value)) {
      throw new VeloxException(String.format("Value %s.%s already added as a sub-factory: ", prefix, value));
    }
    if (classes.containsKey(value)) {
      throw new VeloxException(String.format("Value %s.%s already added as a class: ", prefix, value));
    }
  }
}
