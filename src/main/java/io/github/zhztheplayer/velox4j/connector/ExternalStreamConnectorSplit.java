package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ExternalStreamConnectorSplit extends ConnectorSplit {
  private final long esId;

  @JsonCreator
  public ExternalStreamConnectorSplit(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("esId") long esId) {
    super(connectorId, 0, false);
    this.esId = esId;
  }

  @JsonGetter("esId")
  public long getEsId() {
    return esId;
  }
}
