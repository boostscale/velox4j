package io.github.zhztheplayer.velox4j.variant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.serde.Serde;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class MapValue extends Variant {
  private final Map<Variant, Variant> map;

  public MapValue(Map<Variant, Variant> map) {
    this.map = Collections.unmodifiableMap(map);
  }

  @JsonCreator
  public static MapValue create(@JsonProperty("value") Entries entries) {
    final int size = entries.size();
    final Map<Variant, Variant> builder = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      builder.put(entries.getKeys().get(i), entries.getValues().get(i));
    }
    Preconditions.checkArgument(builder.size() == size,
        "Duplicated keys found in entries while creating MapValue");
    return new MapValue(builder);
  }

  @JsonGetter("value")
  public Entries getEntries() {
    final int size = map.size();
    final List<Variant> keys = new ArrayList<>(size);
    final List<Variant> values = new ArrayList<>(size);
    // The following is basically for test code, to write the map values into JSON with a
    //  comparatively stable order.
    // TODO: This may cause slow serialization as Serde#toJson is called recursively.
    //  A better way is to write reliable #compareTo implementations for all variants.
    final List<Map.Entry<Variant, Variant>> orderedEntries =
        map.entrySet().stream().sorted(Comparator.comparing(o -> Serde.toJson(o.getKey())))
            .collect(Collectors.toList());
    for (Map.Entry<Variant, Variant> entry : orderedEntries) {
      keys.add(entry.getKey());
      values.add(entry.getValue());
    }
    return new Entries(keys, values);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MapValue mapValue = (MapValue) o;
    return Objects.equals(map, mapValue.map);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(map);
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
