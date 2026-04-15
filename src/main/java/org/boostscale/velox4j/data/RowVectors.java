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
package org.boostscale.velox4j.data;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.boostscale.velox4j.jni.JniApi;
import org.boostscale.velox4j.partition.PartitionFunction;
import org.boostscale.velox4j.partition.PartitionFunctionSpec;

public class RowVectors {
  private final JniApi jniApi;

  public RowVectors(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  /**
   * Partitions the input RowVector into a list of RowVectors where each one has the same keys
   * defined by the key indices of `keyChannels`. Uses a default maximum of 128 partitions.
   */
  public List<RowVector> partitionByKeys(RowVector rowVector, List<Integer> keyChannels) {
    return jniApi.rowVectorPartitionByKeys(rowVector, keyChannels, 128);
  }

  /**
   * Partitions a RowVector into numPartitions groups using the given partition function spec.
   * Returns a list of size numPartitions where index i contains rows for partition i (null if
   * empty).
   */
  public List<RowVector> partitionBySpec(
      RowVector rowVector, PartitionFunctionSpec spec, int numPartitions) {
    Preconditions.checkArgument(
        numPartitions > 0, "numPartitions must be positive, got %s", numPartitions);
    try (PartitionFunction partitionFunction =
        jniApi.createPartitionFunction(spec, numPartitions, false)) {
      final int[] partitions = partitionFunction.partition(rowVector);
      return rowVector.wrapPartitions(partitions, numPartitions).stream()
          .map(
              vector -> {
                if (vector == null) {
                  return null;
                }
                return vector.asRowVector();
              })
          .collect(Collectors.toList());
    }
  }
}
