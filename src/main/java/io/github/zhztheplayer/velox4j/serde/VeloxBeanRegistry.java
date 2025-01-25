package io.github.zhztheplayer.velox4j.serde;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

public final class VeloxBeanRegistry {
  private static final Map<String, Class<? extends VeloxBean>> keyToClass = new HashMap<>();
  private static final Map<Class<? extends VeloxBean>, String> classToKey = new HashMap<>();

  static void register(String key, Class<? extends VeloxBean> beanClass) {
    synchronized (VeloxBeanRegistry.class) {
      Preconditions.checkArgument(!keyToClass.containsKey(key),
          "Velox bean type already registered for key: %s", key);
      Preconditions.checkArgument(!classToKey.containsKey(beanClass),
          "Velox bean type already registered for class: %s", beanClass);
      keyToClass.put(key, beanClass);
      classToKey.put(beanClass, key);
    }
  }

  public static Class<? extends VeloxBean> findClassByKey(String key) {
    synchronized (VeloxBeanRegistry.class) {
      Preconditions.checkArgument(keyToClass.containsKey(key),
          "Velox bean type not registered for key: %s", key);
      return keyToClass.get(key);
    }
  }

  public static String findKeyByClass(Class<? extends VeloxBean> beanClass) {
    synchronized (VeloxBeanRegistry.class) {
      Preconditions.checkArgument(classToKey.containsKey(beanClass),
          "Velox bean type not registered for class: %s", beanClass);
      return classToKey.get(beanClass);
    }
  }
}
