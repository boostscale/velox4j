package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MapValue extends Variant {
  private final Entries entries;

  @JsonCreator
  private MapValue(@JsonProperty("value") Entries entries) {
    this.entries = entries;
  }

  public static MapValue create(Map<Variant, Variant> map) {
    final List<Variant> keys = new ArrayList<>(map.size());
    final List<Variant> values = new ArrayList<>(map.size());
    for (Map.Entry<Variant, Variant> entry : map.entrySet()) {
      keys.add(entry.getKey());
      values.add(entry.getValue());
    }
    return new MapValue(new Entries(keys, values));
  }

  @JsonGetter("value")
  public Entries getEntries() {
    return entries;
  }

  public static class Entries {
    private final List<Variant> keys;
    private final List<Variant> values;

    @JsonCreator
    private Entries(@JsonProperty("keys") List<Variant> keys,
        @JsonProperty("values") List<Variant> values) {
      Preconditions.checkArgument(keys.size() == values.size(),
          "Entries should have same number of keys and values");
      Variants.checkSameType(keys);
      Variants.checkSameType(values);
      this.keys = Collections.unmodifiableList(keys);
      this.values = Collections.unmodifiableList(values);
    }

    @JsonGetter("keys")
    public List<Variant> getKeys() {
      return keys;
    }

    @JsonGetter("values")
    public List<Variant> getValues() {
      return values;
    }

    public int size() {
      return keys.size();
    }
  }
}
