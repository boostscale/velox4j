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
import com.google.common.base.Preconditions;

import io.github.zhztheplayer.velox4j.type.RowType;

public class StreamWindowJoinNode extends PlanNode {
  private final List<PlanNode> sources;
  private final PartitionFunctionSpec leftPartFuncSpec;
  private final PartitionFunctionSpec rightPartFuncSpec;
  private final NestedLoopJoinNode probe;
  private final RowType outputType;
  private final int numPartitions;
  private final int leftWindowEndIndex;
  private final int rightWindowEndIndex;

  public StreamWindowJoinNode(
      String id,
      PlanNode leftInput,
      PlanNode rightInput,
      PartitionFunctionSpec leftPartFuncSpec,
      PartitionFunctionSpec rightPartFuncSpec,
      NestedLoopJoinNode probe,
      RowType outputType,
      int numPartitions,
      int leftWindowEndIndex,
      int rightWindowEndIndex) {
    super(id);
    this.sources = List.of(leftInput, rightInput);
    this.leftPartFuncSpec = leftPartFuncSpec;
    this.rightPartFuncSpec = rightPartFuncSpec;
    this.probe = probe;
    this.outputType = outputType;
    this.numPartitions = numPartitions;
    this.leftWindowEndIndex = leftWindowEndIndex;
    this.rightWindowEndIndex = rightWindowEndIndex;
  }

  @JsonCreator
  private static StreamWindowJoinNode create(
      @JsonProperty("id") String id,
      @JsonProperty("sources") List<PlanNode> sources,
      @JsonProperty("leftPartFuncSpec") PartitionFunctionSpec leftPartFuncSpec,
      @JsonProperty("rightPartFuncSpec") PartitionFunctionSpec rightPartFuncSpec,
      @JsonProperty("probe") NestedLoopJoinNode probe,
      @JsonProperty("outputType") RowType outputType,
      @JsonProperty("numPartitions") int numPartitions,
      @JsonProperty("leftWindowEndIndex") int leftWindowEndIndex,
      @JsonProperty("rightWindowEndIndex") int rightWindowEndIndex) {
    Preconditions.checkArgument(
        sources.size() == 2,
        "NestedLoopJoinNode should have 2 sources, but has %s",
        sources.size());
    return new StreamWindowJoinNode(
        id,
        sources.get(0),
        sources.get(1),
        leftPartFuncSpec,
        rightPartFuncSpec,
        probe,
        outputType,
        numPartitions,
        leftWindowEndIndex,
        rightWindowEndIndex);
  }

  @Override
  protected List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("leftPartFuncSpec")
  public PartitionFunctionSpec getLeftPartFuncSpec() {
    return leftPartFuncSpec;
  }

  @JsonGetter("rightPartFuncSpec")
  public PartitionFunctionSpec getRightPartFuncSpec() {
    return rightPartFuncSpec;
  }

  @JsonGetter("probe")
  public NestedLoopJoinNode getProbe() {
    return probe;
  }

  @JsonGetter("outputType")
  public RowType getOutputType() {
    return outputType;
  }

  @JsonGetter("numPartitions")
  public int getNumPartitions() {
    return numPartitions;
  }

  @JsonGetter("leftWindowEndIndex")
  public int getLeftWindowEndIndex() {
    return leftWindowEndIndex;
  }

  @JsonGetter("rightWindowEndIndex")
  public int getRightWindowEndIndex() {
    return rightWindowEndIndex;
  }
}
