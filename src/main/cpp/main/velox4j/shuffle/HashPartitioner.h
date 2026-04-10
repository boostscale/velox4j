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

#pragma once

#include <velox/common/memory/Memory.h>
#include <velox/vector/ComplexVector.h>

namespace velox4j {

/// HashPartitioner partitions a RowVector by hash of key columns. This is
/// the serial-mode equivalent of Velox's PartitionedOutput operator (which
/// requires parallel mode).
///
/// - For in-process repartitioning, use Velox's built-in LocalPartitionNode.
/// - For cross-node shuffle, use HashPartitioner to partition, then serialize
///   each partition separately via VectorSaver.
///
/// Automatically materializes lazy-loaded columns (e.g., from Parquet scans)
/// before partitioning.
class HashPartitioner {
 public:
  /// @param keyChannels Column indices to hash on for partitioning.
  /// @param numPartitions Number of output partitions.
  /// @param pool Memory pool for temporary allocations.
  HashPartitioner(
      std::vector<facebook::velox::column_index_t> keyChannels,
      int numPartitions,
      facebook::velox::memory::MemoryPool* pool);

  /// Hash-partitions the input RowVector. Returns a vector of size
  /// numPartitions where each element is a RowVector for that partition,
  /// or nullptr if the partition is empty.
  std::vector<facebook::velox::RowVectorPtr> partition(
      const facebook::velox::RowVectorPtr& input);

 private:
  /// Materializes lazy columns and computes partition assignments.
  /// Returns the flattened input and fills partitionedRows_ with
  /// per-partition row indices.
  facebook::velox::RowVectorPtr preparePartitions(
      const facebook::velox::RowVectorPtr& input);

  const std::vector<facebook::velox::column_index_t> keyChannels_;
  const int numPartitions_;
  facebook::velox::memory::MemoryPool* const pool_;

  // Reusable state across calls.
  std::vector<uint32_t> partitionIds_;
  std::vector<facebook::velox::vector_size_t> partitionSizes_;
  std::vector<facebook::velox::BufferPtr> partitionRows_;
  std::vector<facebook::velox::vector_size_t*> rawPartitionRows_;
};

} // namespace velox4j
