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

public class StreamWindowPartitionFunctionSpec extends PartitionFunctionSpec {
  private final RowType inputType;
  private final Integer rowtimeIndex;
  private final Long size;
  private final Long step;
  private final Long offset;
  private final Integer windowType;

  @JsonCreator
  public StreamWindowPartitionFunctionSpec(
      @JsonProperty("inputType") RowType inputType,
      @JsonProperty("rowtimeIndex") Integer rowtimeIndex,
      @JsonProperty("size") Long size,
      @JsonProperty("step") Long step,
      @JsonProperty("offset") Long offset,
      @JsonProperty("windowType") Integer windowType) {
    super();
    this.inputType = inputType;
    this.rowtimeIndex = rowtimeIndex;
    this.size = size;
    this.step = step;
    this.offset = offset;
    this.windowType = windowType;
  }

  @JsonGetter("inputType")
  public RowType getInputType() {
    return inputType;
  }

  @JsonGetter("rowtimeIndex")
  public Integer getRowtimeIndex() {
    return rowtimeIndex;
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

}
