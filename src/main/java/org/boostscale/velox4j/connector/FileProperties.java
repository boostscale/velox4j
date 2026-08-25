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
package org.boostscale.velox4j.connector;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FileProperties implements Serializable {
  private final Long fileSize;
  private final Long modificationTime;
  private final Long readRangeHint;
  private final String extraFileInfo;
  private final Map<String, String> fileReadOps;

  @JsonCreator
  public FileProperties(
      @JsonProperty("fileSize") Long fileSize,
      @JsonProperty("modificationTime") Long modificationTime,
      @JsonProperty("readRangeHint") Long readRangeHint,
      @JsonProperty("extraFileInfo") String extraFileInfo,
      @JsonProperty("fileReadOps") Map<String, String> fileReadOps) {
    this.fileSize = fileSize;
    this.modificationTime = modificationTime;
    this.readRangeHint = readRangeHint;
    this.extraFileInfo = extraFileInfo;
    this.fileReadOps = fileReadOps;
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

  @JsonInclude(JsonInclude.Include.ALWAYS)
  @JsonGetter("readRangeHint")
  public Long getReadRangeHint() {
    return readRangeHint;
  }

  @JsonInclude(JsonInclude.Include.ALWAYS)
  @JsonGetter("extraFileInfo")
  public String getExtraFileInfo() {
    return extraFileInfo;
  }

  @JsonInclude(JsonInclude.Include.ALWAYS)
  @JsonGetter("fileReadOps")
  public Map<String, String> getFileReadOps() {
    return fileReadOps;
  }
}
