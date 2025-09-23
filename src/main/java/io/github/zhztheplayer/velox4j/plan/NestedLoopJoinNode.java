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
import com.google.common.base.Preconditions;

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.type.RowType;

import java.util.List;

public class NestedLoopJoinNode extends PlanNode {
  private final List<PlanNode> sources;
  private final JoinType joinType;
  private final TypedExpr joinCondition;
  private final RowType outputType;

  public NestedLoopJoinNode(
      String id,
      JoinType joinType,
      TypedExpr joinCondition,
      PlanNode left,
      PlanNode right,
      RowType outputType) {
    super(id);
    this.sources = List.of(left, right);
    this.joinType = joinType;
    this.joinCondition = joinCondition;
    this.outputType = outputType;
  }

  @JsonCreator
  private static NestedLoopJoinNode create(
      @JsonProperty("id") String id,
      @JsonProperty("joinType") JoinType joinType,
      @JsonProperty("joinCondition") TypedExpr joinCondition,
      @JsonProperty("sources") List<PlanNode> sources,
      @JsonProperty("outputType") RowType outputType) {
    Preconditions.checkArgument(
        sources.size() == 2, "NestedLoopJoinNode should have 2 sources, but has %s", sources.size());
    return new NestedLoopJoinNode(
        id,
        joinType,
        joinCondition,
        sources.get(0),
        sources.get(1),
        outputType);
  }

  @Override
  protected List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("joinType")
  public JoinType getJoinType() {
    return joinType;
  }

  @JsonGetter("joinCondition")
  public TypedExpr getJoinCondition() {
    return joinCondition;
  }

  @JsonGetter("outputType")
  public RowType getOutputType() {
    return outputType;
  }
}
