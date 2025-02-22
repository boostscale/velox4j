package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

// TODO: `dwio::common::WriterOptions` has not serde from Velox so not included
//  in the Java class.
public class HiveInsertTableHandle extends ConnectorInsertTableHandle {
  private final List<HiveColumnHandle> inputColumns;
  private final LocationHandle locationHandle;
  private final FileFormat storageFormat;
  private final HiveBucketProperty bucketProperty;
  private final CompressionKind compressionKind;
  private final Map<String, String> serdeParameters;
  private final boolean ensureFiles;

  @JsonCreator
  public HiveInsertTableHandle(
      @JsonProperty("inputColumns") List<HiveColumnHandle> inputColumns,
      @JsonProperty("locationHandle") LocationHandle locationHandle,
      @JsonProperty("tableStorageFormat") FileFormat storageFormat,
      @JsonProperty("bucketProperty") HiveBucketProperty bucketProperty,
      @JsonProperty("compressionKind") CompressionKind compressionKind,
      @JsonProperty("serdeParameters") Map<String, String> serdeParameters,
      @JsonProperty("ensureFiles") boolean ensureFiles) {
    this.inputColumns = inputColumns;
    this.locationHandle = locationHandle;
    this.storageFormat = storageFormat;
    this.bucketProperty = bucketProperty;
    this.compressionKind = compressionKind;
    this.serdeParameters = serdeParameters;
    this.ensureFiles = ensureFiles;
  }

  @JsonGetter("inputColumns")
  public List<HiveColumnHandle> getInputColumns() {
    return inputColumns;
  }

  @JsonGetter("locationHandle")
  public LocationHandle getLocationHandle() {
    return locationHandle;
  }

  @JsonGetter("tableStorageFormat")
  public FileFormat getStorageFormat() {
    return storageFormat;
  }

  @JsonGetter("bucketProperty")
  public HiveBucketProperty getBucketProperty() {
    return bucketProperty;
  }

  @JsonGetter("compressionKind")
  public CompressionKind getCompressionKind() {
    return compressionKind;
  }

  @JsonGetter("serdeParameters")
  public Map<String, String> getSerdeParameters() {
    return serdeParameters;
  }

  @JsonGetter("ensureFiles")
  public boolean ensureFiles() {
    return ensureFiles;
  }

  @Override
  public boolean supportsMultiThreading() {
    return true;
  }
}
