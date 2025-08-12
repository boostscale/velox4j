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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FileProperties implements Serializable {
  private final Long fileSize;
  private final Long modificationTime;

  @JsonCreator
  public FileProperties(
      @JsonProperty("fileSize") Long fileSize,
      @JsonProperty("modificationTime") Long modificationTime) {
    this.fileSize = fileSize;
    this.modificationTime = modificationTime;
  }

  @JsonInclude(JsonInclude.Include.ALWAYS)
  @JsonGetter("fileSize")
  public Long getFileSize() {
    return fileSize;
  }

  @JsonInclude(JsonInclude.Include.ALWAYS)
  @JsonGetter("modificationTime")
  public Long getModificationTime() {
    return modificationTime;
  }
}
