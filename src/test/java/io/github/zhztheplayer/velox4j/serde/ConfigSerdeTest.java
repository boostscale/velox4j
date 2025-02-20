package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.test.ConfigTests;
import io.github.zhztheplayer.velox4j.test.Velox4jTests;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigSerdeTest {
  @BeforeClass
  public static void beforeClass() {
    Velox4jTests.ensureInitialized();
  }

  @Test
  public void testConfig() {
    final Config config = ConfigTests.randomConfig();
    SerdeTests.testVeloxSerializableRoundTrip(config);
  }

  @Test
  public void testConnectorConfig() {
    final ConnectorConfig connConfig = ConfigTests.randomConnectorConfig();
    SerdeTests.testVeloxSerializableRoundTrip(connConfig);
  }
}
