package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonGetter;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

public abstract class ConnectorSplit extends ISerializable {
  private final String connectorId;
  private final long splitWeight;
  private final boolean cacheable;

  protected ConnectorSplit(String connectorId, long splitWeight, boolean cacheable) {
    this.connectorId = connectorId;
    this.splitWeight = splitWeight;
    this.cacheable = cacheable;
  }

  @JsonGetter("connectorId")
  public String getConnectorId() {
    return connectorId;
  }

  public long getSplitWeight() {
    return splitWeight;
  }

  public boolean isCacheable() {
    return cacheable;
  }
}
