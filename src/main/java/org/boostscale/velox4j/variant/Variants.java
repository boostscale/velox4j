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
package org.boostscale.velox4j.variant;

import java.util.List;

import org.boostscale.velox4j.data.BaseVector;
import org.boostscale.velox4j.jni.JniApi;
import org.boostscale.velox4j.jni.StaticJniApi;
import org.boostscale.velox4j.type.Type;

public class Variants {
  private final JniApi jniApi;

  public Variants(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public VariantCo asCpp(Variant variant) {
    return jniApi.variantAsCpp(variant);
  }

  public BaseVector toVector(Type type, Variant variant) {
    return jniApi.variantToVector(type, variant);
  }

  public Type inferType(Variant variant) {
    return StaticJniApi.get().variantInferType(variant);
  }

  public static void checkSameType(List<Variant> variants) {
    if (variants.size() <= 1) {
      return;
    }
    for (int i = 1; i < variants.size(); i++) {
      if (variants.get(i).getClass() != variants.get(i - 1).getClass()) {
        throw new IllegalArgumentException("All variant values should have same type");
      }
    }
  }
}
