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
package org.boostscale.velox4j.partition;

import org.boostscale.velox4j.data.RowVector;
import org.boostscale.velox4j.jni.CppObject;
import org.boostscale.velox4j.jni.JniApi;

/** Runtime Velox partition function created from a {@link PartitionFunctionSpec}. */
public class PartitionFunction implements CppObject {
  private final JniApi jniApi;
  private final long id;

  public PartitionFunction(JniApi jniApi, long id) {
    this.jniApi = jniApi;
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  public int[] partition(RowVector rowVector) {
    return jniApi.partitionFunctionPartition(this, rowVector);
  }
}
