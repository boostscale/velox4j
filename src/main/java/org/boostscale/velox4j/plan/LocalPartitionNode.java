/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.boostscale.velox4j.plan;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import org.boostscale.velox4j.plan.partition.PartitionFunctionSpec;

/**
 * In-process repartitioning plan node. Partitions data using the specified partition function. Can
 * be used to gather data from multiple sources (N-to-1) or repartition across pipelines (N-to-M).
 *
 * <p>This is Velox's built-in local exchange operator. Unlike {@code ExchangeNode} /{@code
 * PartitionedOutputNode} (which require parallel execution mode for cross-task data transfer),
 * {@code LocalPartitionNode} operates within a single Velox task and is compatible with serial
 * execution mode.
 */
public class LocalPartitionNode extends PlanNode {

  /** The type of local partition. */
  public enum Type {
    /** N-to-1 exchange: gathers data from multiple sources to a single consumer. */
    GATHER("GATHER"),
    /** N-to-M shuffle: repartitions data across multiple consumers. */
    REPARTITION("REPARTITION");

    private final String value;

    Type(String value) {
      this.value = value;
    }

    @JsonValue
    public String toValue() {
      return value;
    }
  }

  private final Type type;
  private final boolean scaleWriter;
  private final PartitionFunctionSpec partitionFunctionSpec;
  private final List<PlanNode> sources;

  @JsonCreator
  public LocalPartitionNode(
      @JsonProperty("id") String id,
      @JsonProperty("type") Type type,
      @JsonProperty("scaleWriter") boolean scaleWriter,
      @JsonProperty("partitionFunctionSpec") PartitionFunctionSpec partitionFunctionSpec,
      @JsonProperty("sources") List<PlanNode> sources) {
    super(id);
    this.type = type;
    this.scaleWriter = scaleWriter;
    this.partitionFunctionSpec = partitionFunctionSpec;
    this.sources = sources;
  }

  @Override
  public List<PlanNode> getSources() {
    return Collections.unmodifiableList(sources);
  }

  @JsonGetter("type")
  public Type getType() {
    return type;
  }

  @JsonGetter("scaleWriter")
  public boolean isScaleWriter() {
    return scaleWriter;
  }

  @JsonGetter("partitionFunctionSpec")
  public PartitionFunctionSpec getPartitionFunctionSpec() {
    return partitionFunctionSpec;
  }
}
