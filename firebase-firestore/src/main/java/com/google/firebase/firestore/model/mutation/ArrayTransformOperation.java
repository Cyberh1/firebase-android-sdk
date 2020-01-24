// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.firebase.firestore.model.mutation;

import static com.google.firebase.firestore.model.value.FieldValue.TYPE_ORDER_ARRAY;

import androidx.annotation.Nullable;
import com.google.firebase.Timestamp;
import com.google.firebase.firestore.model.value.FieldValue;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class used for union and remove array transforms.
 *
 * <p>Implementations are: ArrayTransformOperation.Union and ArrayTransformOperation.Remove
 */
public abstract class ArrayTransformOperation implements TransformOperation {
  private final List<Value> elements;

  ArrayTransformOperation(List<Value> elements) {
    this.elements = Collections.unmodifiableList(elements);
  }

  public List<Value> getElements() {
    return elements;
  }

  @Override
  public FieldValue applyToLocalView(@Nullable FieldValue previousValue, Timestamp localWriteTime) {
    return apply(previousValue);
  }

  @Override
  public FieldValue applyToRemoteDocument(
      @Nullable FieldValue previousValue, FieldValue transformResult) {
    // The server just sends null as the transform result for array operations, so we have to
    // calculate a result the same as we do for local applications.
    return apply(previousValue);
  }

  @Override
  @Nullable
  public FieldValue computeBaseValue(@Nullable FieldValue currentValue) {
    return null; // Array transforms are idempotent and don't require a base value.
  }

  @Override
  @SuppressWarnings("EqualsGetClass") // subtype-sensitive equality is intended.
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArrayTransformOperation that = (ArrayTransformOperation) o;

    return elements.equals(that.elements);
  }

  @Override
  public int hashCode() {
    int result = getClass().hashCode();
    result = 31 * result + elements.hashCode();
    return result;
  }

  /** Applies this ArrayTransformOperation against the specified previousValue. */
  protected abstract FieldValue apply(@Nullable FieldValue previousValue);

  /**
   * Inspects the provided value, returning an ArrayList copy of the internal array if it's an
   * ArrayValue and an empty ArrayList if it's null or any other type of FSTFieldValue.
   */
  static List<Value> coercedFieldValuesArray(@Nullable FieldValue value) {
    if (value.typeOrder() == TYPE_ORDER_ARRAY) {
      return value.getProto().getArrayValue().getValuesList();
    } else {
      // coerce to empty array.
      return new ArrayList<>();
    }
  }

  /** An array union transform operation. */
  public static class Union extends ArrayTransformOperation {
    public Union(List<Value> elements) {
      super(elements);
    }

    @Override
    protected FieldValue apply(@Nullable FieldValue previousValue) {
      List<Value> result = coercedFieldValuesArray(previousValue);
      for (Value element : getElements()) {
        if (!result.contains(element)) {
          result.add(element);
        }
      }
      return FieldValue.of(
          Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(result)).build());
    }
  }

  /** An array remove transform operation. */
  public static class Remove extends ArrayTransformOperation {
    public Remove(List<Value> elements) {
      super(elements);
    }

    @Override
    protected FieldValue apply(@Nullable FieldValue previousValue) {
      List<Value> result = coercedFieldValuesArray(previousValue);
      for (Value element : getElements()) {
        if (!result.contains(element)) {
          result.removeAll(Collections.singleton(element));
        }
      }
      return FieldValue.of(
          Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(result)).build());
    }
  }
}
