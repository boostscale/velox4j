package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.connector.CommitStrategy;
import io.github.zhztheplayer.velox4j.connector.ConnectorInsertTableHandle;
import io.github.zhztheplayer.velox4j.connector.InsertTableHandle;
import io.github.zhztheplayer.velox4j.type.RowType;

import java.util.List;

public class TableWriteNode extends PlanNode {
  private final RowType columns;
  private final List<String> columnNames;
  private final AggregationNode aggregationNode;
  private final InsertTableHandle insertTableHandle;
  private final boolean hasPartitioningScheme;
  private final RowType outputType;
  private final CommitStrategy commitStrategy;
  private final List<PlanNode> sources;

  @JsonCreator
  public TableWriteNode(
      @JsonProperty("id") String id,
      @JsonProperty("columns") RowType columns,
      @JsonProperty("columnNames") List<String> columnNames,
      @JsonProperty("aggregationNode") AggregationNode aggregationNode,
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("connectorInsertTableHandle") ConnectorInsertTableHandle insertTableHandle,
      @JsonProperty("hasPartitioningScheme") boolean hasPartitioningScheme,
      @JsonProperty("outputType") RowType outputType,
      @JsonProperty("commitStrategy") CommitStrategy commitStrategy,
      @JsonProperty("sources") List<PlanNode> sources
  ) {
    super(id);
    this.columns = columns;
    this.columnNames = columnNames;
    this.aggregationNode = aggregationNode;
    this.insertTableHandle = new InsertTableHandle(connectorId, insertTableHandle);
    this.hasPartitioningScheme = hasPartitioningScheme;
    this.outputType = outputType;
    this.commitStrategy = commitStrategy;
    this.sources = sources;
  }

  @JsonGetter("columns")
  public RowType getColumns() {
    return columns;
  }

  @JsonGetter("columnNames")
  public List<String> getColumnNames() {
    return columnNames;
  }

  @JsonGetter("aggregationNode")
  public AggregationNode getAggregationNode() {
    return aggregationNode;
  }

  @JsonGetter("connectorId")
  public String getConnectorId() {
    return insertTableHandle.getConnectorId();
  }

  @JsonGetter("connectorInsertTableHandle")
  public ConnectorInsertTableHandle getInsertTableHandle() {
    return insertTableHandle.connectorInsertTableHandle();
  }

  @JsonGetter("hasPartitioningScheme")
  public boolean hasPartitioningScheme() {
    return hasPartitioningScheme;
  }

  @JsonGetter("outputType")
  public RowType getOutputType() {
    return outputType;
  }

  @JsonGetter("commitStrategy")
  public CommitStrategy getCommitStrategy() {
    return commitStrategy;
  }

  @Override
  protected List<PlanNode> getSources() {
    return sources;
  }
}
