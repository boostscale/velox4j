package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ConfigSerdeTest {
  @BeforeClass
  public static void beforeClass() {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testConfig() {
    final Config config = randomConfig();
    SerdeTests.testVeloxSerializableRoundTrip(config);
  }

  @Test
  public void testConnectorConfig() {
    final ConnectorConfig connConfig = ConnectorConfig.create(
        Map.of(
            "c1", randomConfig(),
            "c5", randomConfig(),
            "c2", randomConfig(),
            "c9", randomConfig())
    );
    SerdeTests.testVeloxSerializableRoundTrip(connConfig);
  }

  private static Config randomConfig() {
    final Map<String, String> entries = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      entries.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }
    final Config config = Config.create(entries);
    return config;
  }
}
