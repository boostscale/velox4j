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

public class DeduplicateNode extends PlanNode{
  private final List<PlanNode> sources;
  private final RowType outputType;
  private Long minRetentionTime;
  private Integer rowtimeIndex;
  private Boolean generateUpdateBefore;
  private Boolean generateInsert;
  private Boolean keepLastRow;

  @JsonCreator
  public DeduplicateNode(
      @JsonProperty("id") String id,
      @JsonProperty("sources") List<PlanNode> sources,
      @JsonProperty("outputType") RowType outputType,
      @JsonProperty("minRetentionTime") Long minRetentionTime,
      @JsonProperty("rowtimeIndex") Integer rowtimeIndex,
      @JsonProperty("generateUpdateBefore") Boolean generateUpdateBefore,
      @JsonProperty("generateInsert") Boolean generateInsert,
      @JsonProperty("keepLastRow") Boolean keepLastRow) {
    super(id);
    this.sources = sources;
    this.outputType = outputType;
    this.minRetentionTime = minRetentionTime;
    this.rowtimeIndex = rowtimeIndex;
    this.generateUpdateBefore = generateUpdateBefore;
    this.generateInsert = generateInsert;
    this.keepLastRow = keepLastRow;
  }

  @Override
  protected List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("outputType")
  public RowType getOutputType() {
    return outputType;
  }

  @JsonGetter("minRetentionTime")
  public Long getMinRetentionTime() {
    return minRetentionTime;
  }

  @JsonGetter("rowtimeIndex")
  public Integer getRowtimeIndex() {
    return rowtimeIndex;
  }

  @JsonGetter("generateUpdateBefore")
  public Boolean generateUpdateBefore() {
    return generateUpdateBefore;
  }

  @JsonGetter("generateInsert")
  public Boolean generateInsert() {
    return generateInsert;
  }

  @JsonGetter("keepLastRow")
  public Boolean keepLastRow() {
    return keepLastRow;
  }
}
