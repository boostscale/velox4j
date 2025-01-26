package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HiveBucketConversion {
  private final int tableBucketCount;
  private final int partitionBucketCount;
  private final List<HiveColumnHandle> bucketColumnHandles;

  @JsonCreator
  public HiveBucketConversion(@JsonProperty("tableBucketCount") int tableBucketCount,
      @JsonProperty("partitionBucketCount") int partitionBucketCount,
      @JsonProperty("bucketColumnHandles") List<HiveColumnHandle> bucketColumnHandles) {
    this.tableBucketCount = tableBucketCount;
    this.partitionBucketCount = partitionBucketCount;
    this.bucketColumnHandles = bucketColumnHandles;
  }

  @JsonProperty("tableBucketCount")
  public int getTableBucketCount() {
    return tableBucketCount;
  }

  @JsonProperty("partitionBucketCount")
  public int getPartitionBucketCount() {
    return partitionBucketCount;
  }

  @JsonProperty("bucketColumnHandles")
  public List<HiveColumnHandle> getBucketColumnHandles() {
    return bucketColumnHandles;
  }
}
