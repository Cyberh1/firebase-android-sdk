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

import static com.google.firebase.firestore.util.Assert.fail;
import static com.google.firebase.firestore.util.Assert.hardAssert;

import androidx.annotation.Nullable;
import com.google.firebase.Timestamp;
import com.google.firestore.v1.Value;

/**
 * Implements the backend semantics for locally computed NUMERIC_ADD (increment) transforms.
 * Converts all field values to longs or doubles and resolves overflows to
 * Long.MAX_VALUE/Long.MIN_VALUE.
 */
public class NumericIncrementTransformOperation implements TransformOperation {
  private Value operand;

  public NumericIncrementTransformOperation(Value operand) {
    this.operand = operand;
  }

  @Override
  public Value applyToLocalView(@Nullable Value previousValue, Timestamp localWriteTime) {
    Value baseValue = computeBaseValue(previousValue);

    // Return an integer value only if the previous value and the operand is an integer.
    if (baseValue.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE
        && operand.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
      long sum = safeIncrement(baseValue.getIntegerValue(), operandAsLong());
      return Value.newBuilder().setIntegerValue(sum).build();
    } else if (baseValue.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
      double sum = baseValue.getDoubleValue() + operandAsDouble();
      return Value.newBuilder().setDoubleValue(sum).build();
    } else {
      hardAssert(
          baseValue.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE,
          "Expected NumberValue to be of type DoubleValue, but was ",
          previousValue.getClass().getCanonicalName());
      double sum = baseValue.getDoubleValue() + operandAsDouble();
      return Value.newBuilder().setDoubleValue(sum).build();
    }
  }

  @Override
  public Value applyToRemoteDocument(@Nullable Value previousValue, Value transformResult) {
    return transformResult;
  }

  public Value getOperand() {
    return operand;
  }

  /**
   * Inspects the provided value, returning the provided value if it is already a NumberValue,
   * otherwise returning a coerced IntegerValue of 0.
   */
  @Override
  public Value computeBaseValue(@Nullable Value previousValue) {
    if (previousValue == null) {
      return Value.newBuilder().setIntegerValue(0).build();
    }

    if (previousValue.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
      return previousValue;
    }

    if (previousValue.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE) {
      return previousValue;
    }

    return Value.newBuilder().setIntegerValue(0).build();
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
    if (operand.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE) {
      return operand.getDoubleValue();
    } else if (operand.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
      return operand.getIntegerValue();
    } else {
      throw fail(
          "Expected 'operand' to be of Number type, but was "
              + operand.getClass().getCanonicalName());
    }
  }

  private long operandAsLong() {
    if (operand.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE) {
      return (long) operand.getDoubleValue();
    } else if (operand.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
      return operand.getIntegerValue();
    } else {
      throw fail(
          "Expected 'operand' to be of Number type, but was "
              + operand.getClass().getCanonicalName());
    }
  }
}
