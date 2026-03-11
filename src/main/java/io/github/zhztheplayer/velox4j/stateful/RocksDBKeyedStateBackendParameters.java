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
package io.github.zhztheplayer.velox4j.stateful;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.zhztheplayer.velox4j.type.Type;

public class RocksDBKeyedStateBackendParameters extends KeyedStateBackendParameters {

  private long dbHandle;
  private long readOptionHandle;
  private long writeOptionHandle;
  private List<String> states;
  private Map<String, String> stateOperators;
  private Map<String, Long> columnFamilies;
  private Map<String, Type> stateKeys;
  private Map<String, Type> stateValues;
  private Map<String, Type> stateNamespaces;

  @JsonCreator
  public RocksDBKeyedStateBackendParameters(
      @JsonProperty("jobId") String jobId,
      @JsonProperty("operatorId") String operatorId,
      @JsonProperty("stateBackendType") int stateBackendType,
      @JsonProperty("dbHandle") long dbHandle,
      @JsonProperty("readOptionHandle") long readOptionHandle,
      @JsonProperty("writeOptionHandle") long writeOptionHandle,
      @JsonProperty("states") List<String> states,
      @JsonProperty("stateOperators") Map<String, String> stateOperators,
      @JsonProperty("columnFamilies") Map<String, Long> columnFamilies,
      @JsonProperty("stateKeys") Map<String, Type> stateKeys,
      @JsonProperty("stateValues") Map<String, Type> stateValues,
      @JsonProperty("stateNamespaces") Map<String, Type> stateNamespaces) {
    super(jobId, operatorId, stateBackendType);
    this.dbHandle = dbHandle;
    this.readOptionHandle = readOptionHandle;
    this.writeOptionHandle = writeOptionHandle;
    this.states = states;
    this.stateOperators = stateOperators;
    this.columnFamilies = columnFamilies;
    this.stateKeys = stateKeys;
    this.stateValues = stateValues;
    this.stateNamespaces = stateNamespaces;
  }

  @JsonProperty("dbHandle")
  public long getDbHandle() {
    return dbHandle;
  }

  @JsonProperty("readOptionHandle")
  public long getReadOptionHandle() {
    return readOptionHandle;
  }

  @JsonProperty("writeOptionHandle")
  public long getWriteOptionHandle() {
    return writeOptionHandle;
  }

  @JsonProperty("states")
  public List<String> getStates() {
    return states;
  }

  @JsonProperty("stateOperators")
  public Map<String, String> getStateOperators() {
    return stateOperators;
  }

  @JsonProperty("columnFamilies")
  public Map<String, Long> getColumnFamilies() {
    return columnFamilies;
  }

  @JsonProperty("stateKeys")
  public Map<String, Type> getStateKeys() {
    return stateKeys;
  }

  @JsonProperty("stateValues")
  public Map<String, Type> getStateValues() {
    return stateValues;
  }

  @JsonProperty("stateNamespaces")
  public Map<String, Type> getStateNamespaces() {
    return stateNamespaces;
  }
}
