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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.boostscale.velox4j.filter.Filter;

public class SubfieldFilter implements Serializable {
  private final String subfield;
  private final Filter filter;

  @JsonCreator
  public SubfieldFilter(
      @JsonProperty("subfield") String subfield, @JsonProperty("filter") Filter filter) {
    this.subfield = subfield;
    this.filter = filter;
  }

  @JsonGetter("subfield")
  public String getSubfield() {
    return subfield;
  }

  @JsonGetter("filter")
  public Filter getFilter() {
    return filter;
  }
}
