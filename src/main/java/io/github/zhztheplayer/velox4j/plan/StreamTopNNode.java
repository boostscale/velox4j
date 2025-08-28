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

public class StreamTopNNode extends PlanNode{
  private final List<PlanNode> sources;
  private final RowType outputType;
  private PlanNode topN;
  PartitionFunctionSpec sortKeySelectorSpec;
  private Boolean generateUpdateBefore;
  private Boolean outputRankNumber;
  private Long cacheSize;

  @JsonCreator
  public StreamTopNNode(
      @JsonProperty("id") String id,
      @JsonProperty("sources") List<PlanNode> sources,
      @JsonProperty("topN") PlanNode topN,
      @JsonProperty("sortKeySelectorSpec") PartitionFunctionSpec sortKeySelectorSpec,
      @JsonProperty("outputType") RowType outputType,
      @JsonProperty("generateUpdateBefore") Boolean generateUpdateBefore,
      @JsonProperty("outputRankNumber") Boolean outputRankNumber,
      @JsonProperty("cacheSize") Long cacheSize) {
    super(id);
    this.sources = sources;
    this.topN = topN;
    this.sortKeySelectorSpec = sortKeySelectorSpec;
    this.outputType = outputType;
    this.generateUpdateBefore = generateUpdateBefore;
    this.outputRankNumber = outputRankNumber;
    this.cacheSize = cacheSize;
  }

  @Override
  protected List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("outputType")
  public RowType getOutputType() {
    return outputType;
  }

  @JsonGetter("topN")
  public PlanNode getTopN() {
    return topN;
  }

  @JsonGetter("sortKeySelectorSpec")
  public PartitionFunctionSpec getSortKeySelectorSpec() {
    return sortKeySelectorSpec;
  }

  @JsonGetter("generateUpdateBefore")
  public Boolean generateUpdateBefore() {
    return generateUpdateBefore;
  }

  @JsonGetter("outputRankNumber")
  public Boolean outputRankNumber() {
    return outputRankNumber;
  }

  @JsonGetter("cacheSize")
  public Long getCacheSize() {
    return cacheSize;
  }
}
