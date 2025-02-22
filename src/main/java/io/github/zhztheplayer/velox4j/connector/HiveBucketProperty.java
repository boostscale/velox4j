package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.github.zhztheplayer.velox4j.serializable.VeloxSerializable;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

public class HiveBucketProperty extends VeloxSerializable {
  private final Kind kind;
  private final int bucketCount;
  private final List<String> bucketedBy;
  private final List<Type> bucketedTypes;
  private final List<HiveSortingColumn> sortedBy;

  @JsonCreator
  public HiveBucketProperty(
      @JsonProperty("kind") Kind kind,
      @JsonProperty("bucketCount") int bucketCount,
      @JsonProperty("bucketedBy") List<String> bucketedBy,
      @JsonProperty("bucketedTypes") List<Type> bucketedTypes,
      @JsonProperty("sortedBy") List<HiveSortingColumn> sortedBy) {
    this.kind = kind;
    this.bucketCount = bucketCount;
    this.bucketedBy = bucketedBy;
    this.bucketedTypes = bucketedTypes;
    this.sortedBy = sortedBy;
  }

  @JsonGetter("kind")
  public Kind getKind() {
    return kind;
  }

  @JsonGetter("bucketCount")
  public int getBucketCount() {
    return bucketCount;
  }

  @JsonGetter("bucketedBy")
  public List<String> getBucketedBy() {
    return bucketedBy;
  }

  @JsonGetter("bucketedTypes")
  public List<Type> getBucketedTypes() {
    return bucketedTypes;
  }

  @JsonGetter("sortedBy")
  public List<HiveSortingColumn> getSortedBy() {
    return sortedBy;
  }

  public enum Kind {
    HIVE_COMPATIBLE(0),
    PRESTO_NATIVE(1);

    private final int value;

    Kind(int value) {
      this.value = value;
    }

    @JsonValue
    public int toValue() {
      return value;
    }
  }
}
