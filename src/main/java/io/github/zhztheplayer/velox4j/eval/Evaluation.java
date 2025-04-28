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
package io.github.zhztheplayer.velox4j.eval;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.serializable.ISerializable;

public class Evaluation extends ISerializable {
  private final TypedExpr expr;
  private final Config queryConfig;
  private final ConnectorConfig connectorConfig;

  @JsonCreator
  public Evaluation(
      @JsonProperty("expr") TypedExpr expr,
      @JsonProperty("queryConfig") Config queryConfig,
      @JsonProperty("connectorConfig") ConnectorConfig connectorConfig) {
    this.expr = expr;
    this.queryConfig = queryConfig;
    this.connectorConfig = connectorConfig;
  }

  @JsonGetter("expr")
  public TypedExpr expr() {
    return expr;
  }

  @JsonGetter("queryConfig")
  public Config queryConfig() {
    return queryConfig;
  }

  @JsonGetter("connectorConfig")
  public ConnectorConfig connectorConfig() {
    return connectorConfig;
  }
}
