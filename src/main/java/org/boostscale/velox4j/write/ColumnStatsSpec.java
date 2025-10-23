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
package org.boostscale.velox4j.write;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.boostscale.velox4j.aggregate.Aggregate;
import org.boostscale.velox4j.aggregate.AggregateStep;
import org.boostscale.velox4j.expression.FieldAccessTypedExpr;

public class ColumnStatsSpec implements Serializable {
  private final List<FieldAccessTypedExpr> groupingKeys;
  private final AggregateStep aggregationStep;
  private final List<String> aggregateNames;
  private final List<Aggregate> aggregates;

  @JsonCreator
  public ColumnStatsSpec(
      @JsonProperty("groupingKeys") List<FieldAccessTypedExpr> groupingKeys,
      @JsonProperty("aggregationStep") AggregateStep aggregationStep,
      @JsonProperty("aggregateNames") List<String> aggregateNames,
      @JsonProperty("aggregates") List<Aggregate> aggregates) {
    this.groupingKeys = groupingKeys;
    this.aggregationStep = aggregationStep;
    this.aggregateNames = aggregateNames;
    this.aggregates = aggregates;
  }

  @JsonGetter("groupingKeys")
  public List<FieldAccessTypedExpr> getGroupingKeys() {
    return groupingKeys;
  }

  @JsonGetter("aggregationStep")
  public AggregateStep getAggregationStep() {
    return aggregationStep;
  }

  @JsonGetter("aggregateNames")
  public List<String> getAggregateNames() {
    return aggregateNames;
  }

  @JsonGetter("aggregates")
  public List<Aggregate> getAggregates() {
    return aggregates;
  }
}
