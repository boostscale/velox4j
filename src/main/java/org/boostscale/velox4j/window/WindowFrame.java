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
package org.boostscale.velox4j.window;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.boostscale.velox4j.expression.TypedExpr;

public class WindowFrame implements Serializable {
  private final WindowType type;
  private final BoundType startType;
  private final TypedExpr startValue;
  private final BoundType endType;
  private final TypedExpr endValue;

  @JsonCreator
  public WindowFrame(
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

  @JsonGetter("type")
  public WindowType getType() {
    return type;
  }

  @JsonGetter("startType")
  public BoundType getStartType() {
    return startType;
  }

  @JsonGetter("startValue")
  public TypedExpr getStartValue() {
    return startValue;
  }

  @JsonGetter("endType")
  public BoundType getEndType() {
    return endType;
  }

  @JsonGetter("endValue")
  public TypedExpr getEndValue() {
    return endValue;
  }
}
