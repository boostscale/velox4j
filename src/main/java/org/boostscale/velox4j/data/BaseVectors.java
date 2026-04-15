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
import com.google.common.collect.ImmutableList;

import org.boostscale.velox4j.jni.JniApi;
import org.boostscale.velox4j.type.Type;

public class BaseVectors {
  private final JniApi jniApi;

  public BaseVectors(JniApi jniApi) {
    this.jniApi = jniApi;
  }

  public BaseVector createEmpty(Type type) {
    return jniApi.createEmptyBaseVector(type);
  }

  public static String serializeOne(BaseVector vector) {
    return JniApi.baseVectorSerialize(ImmutableList.of(vector));
  }

  public BaseVector deserializeOne(String serialized) {
    final List<BaseVector> vectors = jniApi.baseVectorDeserialize(serialized);
    Preconditions.checkState(
        vectors.size() == 1, String.format("Expected one vector, but got %s", vectors.size()));
    return vectors.get(0);
  }

  public static String serializeAll(List<? extends BaseVector> vectors) {
    return JniApi.baseVectorSerialize(vectors);
  }

  /** Serialize a single vector to raw binary bytes (no Base64). Efficient for network transport. */
  public static byte[] serializeOneToBuf(BaseVector vector) {
    return JniApi.baseVectorSerializeToBuf(ImmutableList.of(vector));
  }

  /**
   * Serialize multiple vectors to raw binary bytes (no Base64). Efficient for network transport.
   */
  public static byte[] serializeAllToBuf(List<? extends BaseVector> vectors) {
    return JniApi.baseVectorSerializeToBuf(vectors);
  }

  public List<BaseVector> deserializeAll(String serialized) {
    return jniApi.baseVectorDeserialize(serialized);
  }

  /** Deserialize a single vector from raw binary bytes (no Base64). */
  public BaseVector deserializeOneFromBuf(byte[] buf) {
    final List<BaseVector> vectors = jniApi.baseVectorDeserializeFromBuf(buf);
    Preconditions.checkState(
        vectors.size() == 1, String.format("Expected one vector, but got %s", vectors.size()));
    return vectors.get(0);
  }

  /** Deserialize multiple vectors from raw binary bytes (no Base64). */
  public List<BaseVector> deserializeAllFromBuf(byte[] buf) {
    return jniApi.baseVectorDeserializeFromBuf(buf);
  }

  public static String toString(List<? extends BaseVector> vectors) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < vectors.size(); i++) {
      sb.append("Vector #").append(i).append(System.lineSeparator());
      sb.append(vectors.get(i).toString()).append(System.lineSeparator());
    }
    return sb.toString();
  }
}
