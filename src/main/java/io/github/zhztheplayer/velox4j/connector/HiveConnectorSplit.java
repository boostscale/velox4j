package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.Optional;

public class HiveConnectorSplit extends ConnectorSplit {
  private final String filePath;
  private final FileFormat fileFormat;
  private final long start;
  private final long length;
  private final Map<String, Optional<String>> partitionKeys;
  private final Optional<Integer> tableBucketNumber;
  private final Optional<HiveBucketConversion> bucketConversion;
  private final Map<String, String> customSplitInfo;
  private final Optional<String> extraFileInfo;
  private final Map<String, String> serdeParameters;
  private final Map<String, String> infoColumns;
  private final Optional<FileProperties> properties;
  private final Optional<RowIdProperties> rowIdProperties;

  @JsonCreator
  public HiveConnectorSplit(
      @JsonProperty("filePath") String filePath,
      @JsonProperty("fileFormat") FileFormat fileFormat,
      @JsonProperty("start") long start,
      @JsonProperty("length") long length,
      @JsonProperty("partitionKeys") Map<String, Optional<String>> partitionKeys,
      @JsonProperty("tableBucketNumber") Optional<Integer> tableBucketNumber,
      @JsonProperty("bucketConversion") Optional<HiveBucketConversion> bucketConversion,
      @JsonProperty("customSplitInfo") Map<String, String> customSplitInfo,
      @JsonProperty("extraFileInfo") Optional<String> extraFileInfo,
      @JsonProperty("serdeParameters") Map<String, String> serdeParameters,
      @JsonProperty("infoColumns") Map<String, String> infoColumns,
      @JsonProperty("properties") Optional<FileProperties> properties,
      @JsonProperty("rowIdProperties") Optional<RowIdProperties> rowIdProperties) {
    this.filePath = Preconditions.checkNotNull(filePath);
    this.fileFormat = Preconditions.checkNotNull(fileFormat);
    this.start = start;
    this.length = length;
    this.partitionKeys = Preconditions.checkNotNull(partitionKeys);
    this.tableBucketNumber = tableBucketNumber;
    this.bucketConversion = bucketConversion;
    this.customSplitInfo = Preconditions.checkNotNull(customSplitInfo);
    this.extraFileInfo = extraFileInfo;
    this.serdeParameters = Preconditions.checkNotNull(serdeParameters);
    this.infoColumns = Preconditions.checkNotNull(infoColumns);
    this.properties = properties;
    this.rowIdProperties = rowIdProperties;
  }
}
