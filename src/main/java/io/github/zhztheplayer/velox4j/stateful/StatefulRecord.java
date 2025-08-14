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

import io.github.zhztheplayer.velox4j.data.RowVector;

public class StatefulRecord extends StatefulElement {
  private final long rowVectorId;
  private final long timestamp;
  private final boolean hasTimestamp;
  private RowVector rowVector;
  private final int key;

  public StatefulRecord(
      String nodeId,
      long rowVectorId,
      long timestamp,
      boolean hasTimestamp,
      int key) {
    super(nodeId);
    this.rowVectorId = rowVectorId;
    this.timestamp = timestamp;
    this.hasTimestamp = hasTimestamp;
    this.key = key;
  }

  public RowVector getRowVector() {
    return rowVector;
  }

  public void setRowVector(RowVector rowVector) {
    this.rowVector = rowVector;
  }

  public long getRowVectorId() {
    return rowVectorId;
  }

  public int getKey() {
    return key;
  }

  @Override
  public boolean isWatermark() {
    return false;
  }

  @Override
  public boolean isRecord() {
    return true;
  }

  public boolean hasTimestamp() {
    return hasTimestamp;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void close() {
    if (rowVector != null) {
      rowVector.close();
    }
  }
}
