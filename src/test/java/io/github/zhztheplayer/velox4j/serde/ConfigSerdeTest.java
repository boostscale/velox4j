package io.github.zhztheplayer.velox4j.serde;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ConfigSerdeTest {
  @BeforeClass
  public static void beforeClass() {
    Velox4j.ensureInitialized();
  }

  @Test
  public void testConfig() {
    final Map<String, String> entries = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      entries.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }
    final Config config = Config.create(entries);
    SerdeTests.testVeloxSerializableRoundTrip(config);
  }
}
