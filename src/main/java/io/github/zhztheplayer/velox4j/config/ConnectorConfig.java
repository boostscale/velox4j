package io.github.zhztheplayer.velox4j.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectorConfig extends ISerializable {
  private static final ConnectorConfig EMPTY = new ConnectorConfig(List.of());

  public static ConnectorConfig empty() {
    return EMPTY;
  }

  private final List<ConnectorConfig.Entry> values;

  @JsonCreator
  private ConnectorConfig(@JsonProperty("values") List<ConnectorConfig.Entry> values) {
    this.values = values;
  }

  public static ConnectorConfig create(Map<String, Config> values) {
    return new ConnectorConfig(values.entrySet().stream()
        .map(e -> new ConnectorConfig.Entry(e.getKey(), e.getValue())).collect(Collectors.toList()));
  }

  @JsonGetter("values")
  public List<ConnectorConfig.Entry> values() {
    return values;
  }

  public static class Entry {
    private final String connectorId;
    private final Config config;

    @JsonCreator
    public Entry(@JsonProperty("connectorId") String connectorId, @JsonProperty("config") Config config) {
      this.connectorId = connectorId;
      this.config = config;
    }

    @JsonGetter("connectorId")
    public String connectorId() {
      return connectorId;
    }

    @JsonGetter("config")
    public Config config() {
      return config;
    }
  }
}
