/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.type.RowType;

import java.util.List;

public class GroupWindowAggregationNode extends PlanNode {
  private final PlanNode aggregation;
  private final PartitionFunctionSpec keySelectorSpec;
  private final PartitionFunctionSpec sliceAssignerSpec;
  private final Long allowedLateness;
  private final boolean produceUpdates;
  private final Integer rowtimeIndex;
  private final boolean isEventTime;
  private final Integer windowType;
  private final RowType outputType;

  @JsonCreator
  public GroupWindowAggregationNode(
          @JsonProperty("id") String id,
          @JsonProperty("aggregation") PlanNode aggregation,
          @JsonProperty("keySelectorSpec") PartitionFunctionSpec keySelectorSpec,
          @JsonProperty("sliceAssignerSpec") PartitionFunctionSpec sliceAssignerSpec,
          @JsonProperty("allowedLateness") Long allowedLateness,
          @JsonProperty("produceUpdates") boolean produceUpdates,
          @JsonProperty("rowtimeIndex") Integer rowtimeIndex,
          @JsonProperty("isEventTime") boolean isEventTime,
          @JsonProperty("windowType") Integer windowType,
          @JsonProperty("outputType") RowType outputType) {
    super(id);
    this.aggregation = aggregation;
    this.keySelectorSpec = keySelectorSpec;
    this.sliceAssignerSpec = sliceAssignerSpec;
    this.allowedLateness = allowedLateness;
    this.produceUpdates = produceUpdates;
    this.rowtimeIndex = rowtimeIndex;
    this.isEventTime = isEventTime;
    this.windowType = windowType;
    this.outputType = outputType;
  }

  @JsonGetter("keySelectorSpec")
  public PartitionFunctionSpec getKeySelectorSpec() {
    return keySelectorSpec;
  }

  @JsonGetter("sliceAssignerSpec")
  public PartitionFunctionSpec getSliceAssignerSpec() {
    return sliceAssignerSpec;
  }

  @JsonGetter("aggregation")
  public PlanNode getAggregation() {
    return aggregation;
  }

  @JsonGetter("allowedLateness")
  public Long getAllowedLateness() {
    return allowedLateness;
  }

  @JsonGetter("produceUpdates")
  public boolean produceUpdates() {
    return produceUpdates;
  }

  @JsonGetter("isEventTime")
  public boolean isEventTime() {
    return isEventTime;
  }

  @JsonGetter("windowType")
  public Integer getWindowType() {
    return windowType;
  }

  @JsonGetter("outputType")
  public RowType getOutputType() {
    return outputType;
  }

  @JsonGetter("rowtimeIndex")
  public Integer getRowtimeIndex() {
    return rowtimeIndex;
  }

  @Override
  protected List<PlanNode> getSources() {
    return List.of();
  }

}
