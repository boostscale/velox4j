package io.github.zhztheplayer.velox4j;

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.exception.VeloxException;
import io.github.zhztheplayer.velox4j.jni.JniLibLoader;
import io.github.zhztheplayer.velox4j.jni.JniWorkspace;
import io.github.zhztheplayer.velox4j.jni.StaticJniApi;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.serializable.ISerializableRegistry;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.variant.VariantRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class Velox4j {
  private static final AtomicBoolean initialized = new AtomicBoolean(false);
  private static final Map<String, String> globalConfMap = new LinkedHashMap<>();

  public static void configure(String key, String value) {
    Preconditions.checkNotNull(key, "Key cannot be null");
    Preconditions.checkNotNull(value, "Value of key %s cannot be null", key);
    synchronized (globalConfMap) {
      if (globalConfMap.containsKey(key)) {
        final String oldValue = Preconditions.checkNotNull(globalConfMap.get(key));
        if (Objects.equals(oldValue, value)) {
          // The configure call actually does nothing. Ignore.
          return;
        }
      }
      // If Velox4J was already initialized, throw.
      if (initialized.get()) {
        throw new VeloxException("Could not change configuration after Velox4J was already initialized");
      }
      // Apply the configuration change.
      globalConfMap.put(key, value);
    }
  }

  public static void initialize() {
    if (!initialized.compareAndSet(false, true)) {
      throw new VeloxException("Velox4J has already been initialized");
    }
    initialize0();
  }

  public static Session newSession(MemoryManager memoryManager) {
    return StaticJniApi.get().createSession(memoryManager);
  }

  private static void initialize0() {
    synchronized (globalConfMap) {
      final Config globalConf = Config.create(globalConfMap);
      JniLibLoader.loadAll(JniWorkspace.getDefault().getSubDir("lib"));
      VariantRegistry.registerAll();
      ISerializableRegistry.registerAll();
      StaticJniApi.get().initialize(globalConf);
    }
  }
}
