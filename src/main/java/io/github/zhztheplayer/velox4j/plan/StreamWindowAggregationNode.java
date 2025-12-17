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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.zhztheplayer.velox4j.type.RowType;

public class StreamWindowAggregationNode extends PlanNode {
  private final PlanNode aggregation;
  private final PlanNode localAgg;
  private final PartitionFunctionSpec keySelectorSpec;
  private final PartitionFunctionSpec sliceAssignerSpec;
  private final Long windowInterval;
  private final boolean useDayLightSaving;
  private final boolean isLocalAgg;
  private final Long size;
  private final Long step;
  private final Long offset;
  private final Integer windowType;
  private final RowType outputType;
  private final boolean isEventTime;
  private final Integer rowtimeIndex;
  private final Integer windowStart;
  private final Integer windowEnd;

  @JsonCreator
  public StreamWindowAggregationNode(
          @JsonProperty("id") String id,
          @JsonProperty("aggregation") PlanNode aggregation,
          @JsonProperty("localAgg") PlanNode localAgg,
          @JsonProperty("keySelectorSpec") PartitionFunctionSpec keySelectorSpec,
          @JsonProperty("sliceAssignerSpec") PartitionFunctionSpec sliceAssignerSpec,
          @JsonProperty("windowInterval") Long windowInterval,
          @JsonProperty("useDayLightSaving") boolean useDayLightSaving,
          @JsonProperty("isLocalAgg") boolean isLocalAgg,
          @JsonProperty("size") Long size,
          @JsonProperty("step") Long step,
          @JsonProperty("offset") Long offset,
          @JsonProperty("windowType") Integer windowType,
          @JsonProperty("outputType") RowType outputType,
          @JsonProperty("isRowTime") boolean isEventTime,
          @JsonProperty("rowtimeIndex") Integer rowtimeIndex,
          @JsonProperty("windowStartIndex") Integer windowStart,
          @JsonProperty("windowEndIndex") Integer windowEnd) {
    super(id);
    this.aggregation = aggregation;
    this.localAgg = localAgg;
    this.keySelectorSpec = keySelectorSpec;
    this.sliceAssignerSpec = sliceAssignerSpec;
    this.windowInterval = windowInterval;
    this.useDayLightSaving = useDayLightSaving;
    this.isLocalAgg = isLocalAgg;
    this.size = size;
    this.step = step;
    this.offset = offset;
    this.windowType = windowType;
    this.outputType = outputType;
    this.isEventTime = isEventTime;
    this.rowtimeIndex = rowtimeIndex;
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
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

  @JsonGetter("localAgg")
  public PlanNode getLocalAgg() {
    return localAgg;
  }

  @JsonGetter("windowInterval")
  public Long getWindowInterval() {
    return windowInterval;
  }

  @JsonGetter("useDayLightSaving")
  public boolean useDayLightSaving() {
    return useDayLightSaving;
  }

  @JsonGetter("isLocalAgg")
  public boolean isLocalAgg() {
    return isLocalAgg;
  }

  @JsonGetter("size")
  public Long getSize() {
    return size;
  }

  @JsonGetter("step")
  public Long getStep() {
    return step;
  }

  @JsonGetter("offset")
  public Long getOffset() {
    return offset;
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

  @JsonProperty("isEventTime")
  public boolean getIsEventTime() {
    return isEventTime;
  }

  @JsonProperty("windowStartIndex")
  public Integer getWindowStart() {
    return windowStart;
  }

  @JsonProperty("windowEndIndex")
  public Integer getWindowEnd() {
    return windowEnd;
  }

  @Override
  protected List<PlanNode> getSources() {
    return List.of();
  }
}
