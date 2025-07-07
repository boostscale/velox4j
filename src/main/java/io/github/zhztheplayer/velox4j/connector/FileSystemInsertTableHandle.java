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
package io.github.zhztheplayer.velox4j.connector;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.zhztheplayer.velox4j.type.RowType;

public class FileSystemInsertTableHandle extends ConnectorInsertTableHandle {

  private String tableName;
  private RowType dataColumns;
  private List<Integer> partitionIndexes;
  private List<String> partitionKeys;
  private Map<String, String> tableParameters;

  @JsonCreator
  public FileSystemInsertTableHandle(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("dataColumns") RowType dataColumns,
      @JsonProperty("partitionKeys") List<String> partitionKeys,
      @JsonProperty("partitionIndexes") List<Integer> partitionIndexes,
      @JsonProperty("tableParameters") Map<String, String> tableParameters) {
    this.tableName = tableName;
    this.dataColumns = dataColumns;
    this.partitionKeys = partitionKeys;
    this.partitionIndexes = partitionIndexes;
    this.tableParameters = tableParameters;
  }

  @JsonProperty("tableName")
  public String getTableName() {
    return tableName;
  }

  @JsonProperty("dataColumns")
  public RowType getDataColumns() {
    return dataColumns;
  }

  @JsonProperty("partitionKeys")
  public List<String> getPartitionKeys() {
    return partitionKeys;
  }

  @JsonProperty("partitionIndexes")
  private List<Integer> getPartitionIndexes() {
    return partitionIndexes;
  }

  @JsonProperty("tableParameters")
  public Map<String, String> getTableParameters() {
    return tableParameters;
  }

  @Override
  public boolean supportsMultiThreading() {
    return false;
  }
}
