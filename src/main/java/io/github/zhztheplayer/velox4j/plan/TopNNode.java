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
* countations under the License.
*/
package io.github.zhztheplayer.velox4j.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.sort.SortOrder;

import java.util.List;

public class TopNNode extends PlanNode {
  private final List<FieldAccessTypedExpr> sortingKeys;
  private final List<SortOrder> sortingOrders;
  private final int count;
  private final boolean partial;

  private final List<PlanNode> sources;

  @JsonCreator
  public TopNNode(
      @JsonProperty("id") String id,
      @JsonProperty("sortingKeys") List<FieldAccessTypedExpr> sortingKeys,
      @JsonProperty("sortingOrders") List<SortOrder> sortingOrders,
      @JsonProperty("count") int count,
      @JsonProperty("partial") boolean partial,
      @JsonProperty("sources") List<PlanNode> sources) {
    super(id);
    this.sortingKeys = sortingKeys;
    this.sortingOrders = sortingOrders;
    this.count = count;
    this.partial = partial;
    this.sources = sources;
  }

  @Override
  public List<PlanNode> getSources() {
    return sources;
  }

  @JsonGetter("sortingKeys")
  public List<FieldAccessTypedExpr> getSortingKeys() {
    return sortingKeys;
  }

  @JsonGetter("sortingOrders")
  public List<SortOrder> getSortingOrders() {
    return sortingOrders;
  }

  @JsonGetter("count")
  public int getCount() {
    return count;
  }

  @JsonGetter("partial")
  public boolean isPartial() {
    return partial;
  }
}
