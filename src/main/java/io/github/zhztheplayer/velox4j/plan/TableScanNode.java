package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.connector.Assignment;
import io.github.zhztheplayer.velox4j.connector.ConnectorTableHandle;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.Collections;
import java.util.List;

public class TableScanNode extends PlanNode {
  private final Type outputType;
  private final ConnectorTableHandle tableHandle;
  private final List<Assignment> assignments;

  @JsonCreator
  public TableScanNode(@JsonProperty("id") String id,
      @JsonProperty("outputType") Type outputType,
      @JsonProperty("tableHandle") ConnectorTableHandle tableHandle,
      @JsonProperty("assignments") List<Assignment> assignments) {
    super(id);
    this.outputType = outputType;
    this.tableHandle = tableHandle;
    this.assignments = assignments;
  }

  @JsonGetter("outputType")
  public Type getOutputType() {
    return outputType;
  }

  @JsonGetter("tableHandle")
  public ConnectorTableHandle getTableHandle() {
    return tableHandle;
  }

  @JsonGetter("assignments")
  public List<Assignment> getAssignments() {
    return assignments;
  }

  @Override
  protected List<PlanNode> getSources() {
    return Collections.emptyList();
  }
}
