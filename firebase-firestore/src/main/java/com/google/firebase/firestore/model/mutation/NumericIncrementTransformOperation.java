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

import static com.google.firebase.firestore.model.value.FieldValue.TYPE_ORDER_NUMBER;
import static com.google.firebase.firestore.model.value.ProtoValues.isType;
import static com.google.firebase.firestore.util.Assert.fail;
import static com.google.firebase.firestore.util.Assert.hardAssert;

import androidx.annotation.Nullable;
import com.google.firebase.Timestamp;
import com.google.firebase.firestore.model.value.FieldValue;
import com.google.firestore.v1.Value;

/**
 * Implements the backend semantics for locally computed NUMERIC_ADD (increment) transforms.
 * Converts all field values to longs or doubles and resolves overflows to
 * Long.MAX_VALUE/Long.MIN_VALUE.
 */
public class NumericIncrementTransformOperation implements TransformOperation {
  private FieldValue operand;

  public NumericIncrementTransformOperation(FieldValue operand) {
    this.operand = operand;
  }

  @Override
  public FieldValue applyToLocalView(@Nullable FieldValue previousValue, Timestamp localWriteTime) {
    FieldValue baseValue = computeBaseValue(previousValue);

    // Return an integer value only if the previous value and the operand is an integer.
    if (isIntegerValue(baseValue) && isIntegerValue(operand)) {
      long sum = safeIncrement(baseValue.getProto().getIntegerValue(), operandAsLong());
      return FieldValue.of(Value.newBuilder().setIntegerValue(sum).build());
    } else if (isIntegerValue(baseValue)) {
      double sum = baseValue.getProto().getIntegerValue() + operandAsDouble();
      return FieldValue.of(Value.newBuilder().setDoubleValue(sum).build());
    } else {
      hardAssert(
          isDoubleValue(baseValue),
          "Expected NumberValue to be of double value, but was ",
          baseValue);
      double sum = baseValue.getProto().getDoubleValue() + operandAsDouble();
      return FieldValue.of(Value.newBuilder().setDoubleValue(sum).build());
    }
  }

  @Override
  public FieldValue applyToRemoteDocument(
      @Nullable FieldValue previousValue, FieldValue transformResult) {
    return transformResult;
  }

  public FieldValue getOperand() {
    return operand;
  }

  /**
   * Inspects the provided value, returning the provided value if it is already a NumberValue,
   * otherwise returning a coerced IntegerValue of 0.
   */
  @Override
  public FieldValue computeBaseValue(@Nullable FieldValue previousValue) {
    return previousValue != null && isType(previousValue.getProto(), TYPE_ORDER_NUMBER)
        ? previousValue
        : FieldValue.of(Value.newBuilder().setIntegerValue(0).build());
  }

  /**
   * Implementation of Java 8's `addExact()` that resolves positive and negative numeric overflows
   * to Long.MAX_VALUE or Long.MIN_VALUE respectively (instead of throwing an ArithmeticException).
   */
  private long safeIncrement(long x, long y) {
    long r = x + y;

    // See "Hacker's Delight" 2-12: Overflow if both arguments have the opposite sign of the result
    if (((x ^ r) & (y ^ r)) >= 0) {
      return r;
    }

    if (r >= 0L) {
      return Long.MIN_VALUE;
    } else {
      return Long.MAX_VALUE;
    }
  }

  private double operandAsDouble() {
    if (isDoubleValue(operand)) {
      return operand.getProto().getDoubleValue();
    } else if (isIntegerValue(operand)) {
      return operand.getProto().getIntegerValue();
    } else {
      throw fail(
          "Expected 'operand' to be of Number type, but was "
              + operand.getClass().getCanonicalName());
    }
  }

  private long operandAsLong() {
    if (isDoubleValue(operand)) {
      return (long) operand.getProto().getDoubleValue();
    } else if (isIntegerValue(operand)) {
      return operand.getProto().getIntegerValue();
    } else {
      throw fail(
          "Expected 'operand' to be of Number type, but was "
              + operand.getClass().getCanonicalName());
    }
  }

  private boolean isIntegerValue(@Nullable FieldValue value) {
    return value != null && value.getProto().getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE;
  }

  private boolean isDoubleValue(@Nullable FieldValue value) {
    return value != null && value.getProto().getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE;
  }
}
