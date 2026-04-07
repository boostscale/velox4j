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

#include "velox4j/shuffle/ShuffleWriter.h"

#include <velox/exec/HashPartitionFunction.h>
#include <velox/exec/OperatorUtils.h>
#include <velox/vector/VectorSaver.h>

#include "velox4j/vector/Vectors.h"

namespace velox4j {

using namespace facebook::velox;

ShuffleWriter::ShuffleWriter(
    std::vector<column_index_t> keyChannels,
    int numPartitions,
    memory::MemoryPool* pool)
    : keyChannels_(std::move(keyChannels)),
      numPartitions_(numPartitions),
      pool_(pool) {
  VELOX_USER_CHECK_GT(numPartitions_, 0, "numPartitions must be positive");
}

RowVectorPtr ShuffleWriter::preparePartitions(const RowVectorPtr& input) {
  // Materialize lazy columns before partitioning. exec::wrap() creates
  // dictionary views over the original vector — if it has lazy columns
  // (e.g. from a Parquet scan), the wraps inherit them and will fail
  // when accessed outside the scan's lifecycle.
  VectorPtr vec = std::dynamic_pointer_cast<BaseVector>(input);
  flattenVector(vec, input->size());
  auto flattened = std::dynamic_pointer_cast<RowVector>(vec);
  const auto numRows = flattened->size();

  // Compute partition assignment for each row.
  exec::HashPartitionFunction hashFunc(
      false, numPartitions_, asRowType(flattened->type()), keyChannels_);
  partitionIds_.resize(numRows);
  hashFunc.partition(*flattened, partitionIds_);

  // Count rows per partition.
  partitionSizes_.assign(numPartitions_, 0);
  for (auto row = 0; row < numRows; ++row) {
    ++partitionSizes_[partitionIds_[row]];
  }

  // Allocate row index buffers per partition.
  partitionRows_.resize(numPartitions_);
  rawPartitionRows_.resize(numPartitions_);
  for (int pid = 0; pid < numPartitions_; ++pid) {
    partitionRows_[pid] = allocateIndices(partitionSizes_[pid], pool_);
    rawPartitionRows_[pid] = partitionRows_[pid]->asMutable<vector_size_t>();
  }

  // Fill row indices per partition.
  std::vector<vector_size_t> offsets(numPartitions_, 0);
  for (auto row = 0; row < numRows; ++row) {
    const auto pid = partitionIds_[row];
    rawPartitionRows_[pid][offsets[pid]] = row;
    ++offsets[pid];
  }

  return flattened;
}

std::vector<RowVectorPtr> ShuffleWriter::partition(const RowVectorPtr& input) {
  auto flattened = preparePartitions(input);
  const auto numRows = flattened->size();

  std::vector<RowVectorPtr> result(numPartitions_);
  for (int pid = 0; pid < numPartitions_; ++pid) {
    if (partitionSizes_[pid] == 0) {
      continue;
    }
    result[pid] = partitionSizes_[pid] == numRows
        ? flattened
        : exec::wrap(partitionSizes_[pid], partitionRows_[pid], flattened);
  }
  return result;
}

std::vector<std::string> ShuffleWriter::partitionAndSerialize(
    const RowVectorPtr& input) {
  auto flattened = preparePartitions(input);
  const auto numRows = flattened->size();

  std::vector<std::string> result(numPartitions_);
  for (int pid = 0; pid < numPartitions_; ++pid) {
    if (partitionSizes_[pid] == 0) {
      continue;
    }
    const RowVectorPtr rowVector = partitionSizes_[pid] == numRows
        ? flattened
        : exec::wrap(partitionSizes_[pid], partitionRows_[pid], flattened);

    std::ostringstream out;
    saveVector(*rowVector, out);
    result[pid] = out.str();
  }
  return result;
}

} // namespace velox4j
