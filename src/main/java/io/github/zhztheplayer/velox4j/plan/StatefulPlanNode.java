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

import java.util.ArrayList;
import java.util.List;

public class StatefulPlanNode extends PlanNode {
  private final PlanNode node;
  private List<StatefulPlanNode> targets;

  @JsonCreator
  public StatefulPlanNode(
      @JsonProperty("id") String id,
      @JsonProperty("node") PlanNode node) {
    super(id);
    this.node = node;
  }

  @Override
  protected List<PlanNode> getSources() {
    return List.of();
  }

  @JsonGetter("node")
  public PlanNode getNode() {
    return node;
  }

  @JsonGetter("targets")
  //@JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<StatefulPlanNode> getTargets() {
    return targets;
  }

  public void addTarget(StatefulPlanNode target) {
    if (targets == null) {
      targets = new ArrayList<>();
    }
    targets.add(target);
  }
}
