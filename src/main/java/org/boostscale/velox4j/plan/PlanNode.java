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
package org.boostscale.velox4j.plan;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;

import org.boostscale.velox4j.jni.StaticJniApi;
import org.boostscale.velox4j.serializable.ISerializable;
import org.boostscale.velox4j.serializable.ISerializableCo;
import org.boostscale.velox4j.session.Session;

public abstract class PlanNode extends ISerializable {
  private final String id;

  protected PlanNode(String id) {
    this.id = id;
  }

  @JsonGetter("id")
  public String getId() {
    return id;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonGetter("sources")
  public abstract List<PlanNode> getSources();

  /**
   * Returns a human-readable string representation of this plan tree using Velox's C++ formatting.
   *
   * @param session the session used to send the plan to C++ for formatting
   * @param detailed if true, includes node-specific details (expressions, join keys, etc.)
   * @param recursive if true, includes the entire subtree; otherwise just this node
   */
  public String toFormatString(Session session, boolean detailed, boolean recursive) {
    try (ISerializableCo co = session.iSerializableOps().asCpp(this)) {
      return StaticJniApi.get().planNodeToString(co, detailed, recursive);
    }
  }
}
