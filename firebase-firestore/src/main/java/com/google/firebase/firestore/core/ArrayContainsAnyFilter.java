// Copyright 2019 Google LLC
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

package com.google.firebase.firestore.core;

import com.google.firebase.firestore.model.Document;
import com.google.firebase.firestore.model.FieldPath;
import com.google.firestore.v1.Value;
import java.util.List;

/** A Filter that implements the array-contains-any operator. */
public class ArrayContainsAnyFilter extends FieldFilter {
  ArrayContainsAnyFilter(FieldPath field, Value value) {
    super(field, Operator.ARRAY_CONTAINS_ANY, value);
  }

  @Override
  public boolean matches(Document doc) {
    Value other = doc.getField(getField());
    if (other == null || other.getValueTypeCase() != Value.ValueTypeCase.ARRAY_VALUE) {
      return false;
    }
    List<Value> valuesList = getValue().getArrayValue().getValuesList();
    for (Value val : other.getArrayValue().getValuesList()) {
      if (valuesList.contains(val)) {
        return true;
      }
    }
    return false;
  }
}
