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
package io.github.zhztheplayer.velox4j.window;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

public class Frame extends ISerializable {
  private final WindowType type;
  private final BoundType startType;
  private final TypedExpr startValue;
  private final BoundType endType;
  private final TypedExpr endValue;

  @JsonCreator
  public Frame(
      @JsonProperty("type") WindowType type,
      @JsonProperty("startType") BoundType startType,
      @JsonProperty("startValue") TypedExpr startValue,
      @JsonProperty("endType") BoundType endType,
      @JsonProperty("endValue") TypedExpr endValue) {
    this.type = type;
    this.startType = startType;
    this.startValue = startValue;
    this.endType = endType;
    this.endValue = endValue;
  }

  @JsonProperty("type")
  public WindowType getType() {
    return type;
  }

  @JsonProperty("startType")
  public BoundType getStartType() {
    return startType;
  }

  @JsonProperty("startValue")
  public TypedExpr getStartValue() {
    return startValue;
  }

  @JsonProperty("endType")
  public BoundType getEndType() {
    return endType;
  }

  @JsonProperty("endValue")
  public TypedExpr getEndValue() {
    return endValue;
  }
}
