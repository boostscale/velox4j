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
package org.boostscale.velox4j.plan.partition;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.boostscale.velox4j.expression.ConstantTypedExpr;
import org.boostscale.velox4j.type.RowType;

/**
 * Hash partition function that distributes data based on hash values of specified key columns. The
 * same key always maps to the same partition — suitable for distributed shuffle joins.
 */
public class HashPartitionFunctionSpec extends PartitionFunctionSpec {
  private final RowType inputType;
  private final List<Integer> keyChannels;
  private final List<ConstantTypedExpr> constants;

  @JsonCreator
  public HashPartitionFunctionSpec(
      @JsonProperty("inputType") RowType inputType,
      @JsonProperty("keyChannels") List<Integer> keyChannels,
      @JsonProperty("constants") List<ConstantTypedExpr> constants) {
    this.inputType = inputType;
    this.keyChannels = keyChannels;
    this.constants = constants;
  }

  public HashPartitionFunctionSpec(RowType inputType, List<Integer> keyChannels) {
    this(inputType, keyChannels, Collections.emptyList());
  }

  @JsonGetter("inputType")
  public RowType getInputType() {
    return inputType;
  }

  @JsonGetter("keyChannels")
  public List<Integer> getKeyChannels() {
    return keyChannels;
  }

  @JsonGetter("constants")
  public List<ConstantTypedExpr> getConstants() {
    return constants;
  }
}
