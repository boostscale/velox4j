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

import com.google.common.base.Preconditions;

import org.boostscale.velox4j.jni.JniApi;

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
   * Partitions the input RowVector into a list of RowVectors where each one has the same keys
   * defined by the key indices of `keyChannels`, with a configurable maximum number of partitions.
   */
  public List<RowVector> partitionByKeys(
      RowVector rowVector, List<Integer> keyChannels, int maxPartitions) {
    Preconditions.checkArgument(
        maxPartitions > 0, "maxPartitions must be positive, got %s", maxPartitions);
    return jniApi.rowVectorPartitionByKeys(rowVector, keyChannels, maxPartitions);
  }

  /**
   * Hash-partitions a RowVector into numPartitions groups using consistent hashing. The same key
   * always maps to the same partition regardless of which node computes it. Returns a list of size
   * numPartitions where index i contains rows for partition i (null if empty).
   */
  public List<RowVector> hashPartition(
      RowVector rowVector, List<Integer> keyChannels, int numPartitions) {
    Preconditions.checkArgument(
        numPartitions > 0, "numPartitions must be positive, got %s", numPartitions);
    return jniApi.rowVectorHashPartition(rowVector, keyChannels, numPartitions);
  }

  /**
   * Hash-partitions and serializes a RowVector. Returns byte[][] indexed by partition number. Null
   * entries for empty partitions. Uses Velox native binary format (compatible with {@link
   * BaseVectors#deserializeOneFromBuf}).
   */
  public byte[][] hashPartitionAndSerialize(
      RowVector rowVector, List<Integer> keyChannels, int numPartitions) {
    Preconditions.checkArgument(
        numPartitions > 0, "numPartitions must be positive, got %s", numPartitions);
    return jniApi.rowVectorHashPartitionAndSerialize(rowVector, keyChannels, numPartitions);
  }
}
