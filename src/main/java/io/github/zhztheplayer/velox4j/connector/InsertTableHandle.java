package io.github.zhztheplayer.velox4j.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InsertTableHandle {
  private final String connectorId;
  private final ConnectorInsertTableHandle connectorInsertTableHandle;

  @JsonCreator
  public InsertTableHandle(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("connectorInsertTableHandle") ConnectorInsertTableHandle connectorInsertTableHandle) {
    this.connectorId = connectorId;
    this.connectorInsertTableHandle = connectorInsertTableHandle;
  }

  @JsonGetter("connectorId")
  public String getConnectorId() {
    return connectorId;
  }

  @JsonGetter("connectorInsertTableHandle")
  public ConnectorInsertTableHandle connectorInsertTableHandle() {
    return connectorInsertTableHandle;
  }
}
