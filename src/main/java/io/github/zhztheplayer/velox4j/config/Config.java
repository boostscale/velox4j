package io.github.zhztheplayer.velox4j.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Config extends ISerializable {
  private static final Config EMPTY = new Config(List.of());

  public static Config empty() {
    return EMPTY;
  }

  private final List<Entry> values;

  @JsonCreator
  private Config(@JsonProperty("values") List<Entry> values) {
    this.values = values;
  }

  public static Config create(Map<String, String> values) {
    return new Config(values.entrySet().stream()
        .map(e -> new Entry(e.getKey(), e.getValue())).collect(Collectors.toList()));
  }

  @JsonGetter("values")
  public List<Entry> values() {
    return values;
  }

  public static class Entry {
    private final String key;
    private final String value;

    @JsonCreator
    public Entry(@JsonProperty("key") String key, @JsonProperty("value") String value) {
      this.key = key;
      this.value = value;
    }

    @JsonGetter("key")
    public String key() {
      return key;
    }

    @JsonGetter("value")
    public String value() {
      return value;
    }
  }
}
