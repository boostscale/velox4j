package io.github.zhztheplayer.velox4j.test;

import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class ConfigTests {
  private ConfigTests() {}

  public static Config randomConfig() {
    final Map<String, String> entries = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      entries.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }
    final Config config = Config.create(entries);
    return config;
  }

  public static ConnectorConfig randomConnectorConfig() {
    final Map<String, Config> values = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      values.put(UUID.randomUUID().toString(), randomConfig());
    }
    return ConnectorConfig.create(values);
  }
}
