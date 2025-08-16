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

#include "Vectors.h"

#include <velox/vector/ComplexVector.h>
#include <velox/vector/LazyVector.h>

namespace velox4j {
using namespace facebook::velox;

namespace {
vector_size_t getTargetSizeInElements(
    const ArrayVectorBase* vector,
    size_t targetSize) {
  VELOX_CHECK_NOT_NULL(vector);
  if (targetSize == 0) {
    return 0;
  }
  return vector->offsetAt(targetSize - 1) + vector->sizeAt(targetSize - 1);
}
} // namespace

void flattenVector(VectorPtr& vector, vector_size_t targetSize) {
  if (!vector) {
    return;
  }
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      break;
    case VectorEncoding::Simple::ROW: {
      auto* rowVector = vector->asUnchecked<RowVector>();
      for (auto& child : rowVector->children()) {
        flattenVector(child, targetSize);
      }
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* arrayVector = vector->asUnchecked<ArrayVector>();
      auto targetSizeInElements =
          getTargetSizeInElements(arrayVector, targetSize);
      flattenVector(arrayVector->elements(), targetSizeInElements);
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto* mapVector = vector->asUnchecked<MapVector>();
      auto targetSizeInElements =
          getTargetSizeInElements(mapVector, targetSize);
      flattenVector(mapVector->mapKeys(), targetSizeInElements);
      flattenVector(mapVector->mapValues(), targetSizeInElements);
      break;
    }
    case VectorEncoding::Simple::LAZY: {
      vector = vector->asUnchecked<LazyVector>()->loadedVectorShared();
      flattenVector(vector, targetSize);
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
} // namespace velox4j
