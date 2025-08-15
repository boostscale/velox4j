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

#include "velox4j/arrow/Arrow.h"
#include <arrow/c/helpers.h>
#include <velox/vector/LazyVector.h>
#include <velox/vector/arrow/Bridge.h>

namespace velox4j {
using namespace facebook::velox;

namespace {
// A customized version of `BaseVector::flattenVector`,
// mainly to workaround https://github.com/facebookincubator/velox/issues/14492.
// We also truncate all vectors at `targetSize`, because some Velox operations
// (E.g., Limit) could result in a RowVector whose children have larger size
// than itself. So we perform a slice to keep only the data that is
// in real use.
void flatten(VectorPtr& vector, size_t targetSize) {
  if (!vector) {
    return;
  }
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      break;
    case VectorEncoding::Simple::ROW: {
      auto* rowVector = vector->asUnchecked<RowVector>();
      for (auto& child : rowVector->children()) {
        flatten(child, targetSize);
      }
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* arrayVector = vector->asUnchecked<ArrayVector>();
      flatten(arrayVector->elements(), targetSize);
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto* mapVector = vector->asUnchecked<MapVector>();
      flatten(mapVector->mapKeys(), targetSize);
      flatten(mapVector->mapValues(), targetSize);
      break;
    }
    case VectorEncoding::Simple::LAZY: {
      vector = vector->asUnchecked<LazyVector>()->loadedVectorShared();
      flatten(vector, targetSize);
      break;
    }
    default:
      BaseVector::ensureWritable(
          SelectivityVector::empty(), vector->type(), vector->pool(), vector);
  }
  VELOX_CHECK_GE(vector->size(), targetSize);
  if (vector->size() > targetSize) {
    vector = vector->slice(0, targetSize);
  }
}

ArrowOptions makeOptions() {
  ArrowOptions options;
  // TODO: Make the timestamp unit configurable or included in presets.
  options.timestampUnit = static_cast<TimestampUnit>(6);
  return options;
}
} // namespace

void fromTypeToArrow(
    memory::MemoryPool* pool,
    TypePtr type,
    ArrowSchema* cSchema) {
  auto options = makeOptions();
  auto emptyVector = BaseVector::create(type, 0, pool);
  exportToArrow(emptyVector, *cSchema, options);
}

TypePtr fromArrowToType(ArrowSchema* cSchema) {
  auto options = makeOptions();
  auto type = importFromArrow(*cSchema);
  ArrowSchemaRelease(cSchema);
  return type;
}

void fromBaseVectorToArrow(
    VectorPtr vector,
    ArrowSchema* cSchema,
    ArrowArray* cArray) {
  flatten(vector, vector->size());
  auto options = makeOptions();
  exportToArrow(vector, *cSchema, options);
  exportToArrow(vector, *cArray, vector->pool(), options);
}

VectorPtr fromArrowToBaseVector(
    memory::MemoryPool* pool,
    ArrowSchema* cSchema,
    ArrowArray* cArray) {
  auto options = makeOptions();
  auto vector = importFromArrowAsOwner(*cSchema, *cArray, pool);
  ArrowSchemaRelease(cSchema);
  ArrowArrayRelease(cArray);
  return vector;
}
} // namespace velox4j
