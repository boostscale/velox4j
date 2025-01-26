package io.github.zhztheplayer.velox4j.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class RowIdProperties {
  private final long metadataVersion;
  private final long partitionId;
  private final String tableGuid;

  @JsonCreator
  public RowIdProperties(@JsonProperty("metadataVersion") long metadataVersion,
      @JsonProperty("partitionId") long partitionId,
      @JsonProperty("tableGuid") String tableGuid) {
    this.metadataVersion = metadataVersion;
    this.partitionId = partitionId;
    this.tableGuid = Preconditions.checkNotNull(tableGuid);
  }

  @JsonGetter("metadataVersion")
  public long getMetadataVersion() {
    return metadataVersion;
  }

  @JsonGetter("partitionId")
  public long getPartitionId() {
    return partitionId;
  }

  @JsonGetter("tableGuid")
  public String getTableGuid() {
    return tableGuid;
  }
}
