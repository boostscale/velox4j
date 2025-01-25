package io.github.zhztheplayer.velox4j.serde;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public abstract class SerdeRegistry<T> {
  private final Map<String, Class<? extends T>> keyToClass = new HashMap<>();
  private final Map<Class<? extends T>, String> classToKey = new HashMap<>();

  public void register(String key, Class<? extends T> beanClass) {
    synchronized (this) {
      Preconditions.checkArgument(!keyToClass.containsKey(key),
          "[%s] Type already registered for key: %s", this.getClass(), key);
      Preconditions.checkArgument(!classToKey.containsKey(beanClass),
          "[%s] Type already registered for class: %s", this.getClass(), beanClass);
      keyToClass.put(key, beanClass);
      classToKey.put(beanClass, key);
    }
  }

  public Class<? extends T> findClassByKey(String key) {
    synchronized (this) {
      Preconditions.checkArgument(keyToClass.containsKey(key),
          "[%s] Type not registered for key: %s", this.getClass(), key);
      return keyToClass.get(key);
    }
  }

  public String findKeyByClass(Class<? extends T> beanClass) {
    synchronized (this) {
      Preconditions.checkArgument(classToKey.containsKey(beanClass),
          "[%s] Type not registered for class: %s", this.getClass(), beanClass);
      return classToKey.get(beanClass);
    }
  }
}
