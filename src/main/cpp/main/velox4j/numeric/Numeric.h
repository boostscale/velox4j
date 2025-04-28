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

#pragma once

#include <velox/common/base/Exceptions.h>

namespace velox4j {
template <typename T, typename F>
T safeCast(F f) {
  constexpr T tMin = std::numeric_limits<T>::min();
  constexpr T tMax = std::numeric_limits<T>::max();
  constexpr F fMin = std::numeric_limits<F>::min();
  constexpr F fMax = std::numeric_limits<F>::max();
  static_assert(tMin > fMin || tMax < fMax, "Vain safe casting");

  VELOX_CHECK(
      f >= tMin,
      "Number overflows lower bound: {} < {}",
      std::to_string(f),
      std::to_string(tMin));
  VELOX_CHECK(
      f <= tMax,
      "Number overflows upper bound: {} > {}",
      std::to_string(f),
      std::to_string(tMax));
  return static_cast<T>(f);
}
} // namespace velox4j
