/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.io.reader;

import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.table.PrimaryKeySpec;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class FieldMergeOperators {

  public static FieldMergeOperator operator(
      String name,
      Type type,
      BiFunction<Type, Object, Object> convertFromFunction,
      BiFunction<Type, Object, Object> convertToFunction) {
    switch (name) {
      case FieldMergeOperator.LAST:
        return FieldMergeOperator.Last.get();
      case FieldMergeOperator.LAST_NON_NULL:
        return FieldMergeOperator.LastNonNull.get();
      case FieldMergeOperator.FIRST:
        return FieldMergeOperator.First.get();
      case FieldMergeOperator.FIRST_NON_NULL:
        return FieldMergeOperator.FirstNonNull.get();
      case FieldMergeOperator.SUM:
        return new FieldMergeOperator.Sum(type, convertFromFunction, convertToFunction);
      case FieldMergeOperator.PRODUCT:
        return new FieldMergeOperator.Product(type, convertFromFunction, convertToFunction);
      case FieldMergeOperator.MAX:
        return new FieldMergeOperator.Max(type, convertFromFunction, convertToFunction);
      case FieldMergeOperator.MIN:
        return new FieldMergeOperator.Min(type, convertFromFunction, convertToFunction);
      case FieldMergeOperator.BOOL_AND:
        return new FieldMergeOperator.BoolAnd(type, convertFromFunction, convertToFunction);
      case FieldMergeOperator.BOOL_OR:
        return new FieldMergeOperator.BoolOr(type, convertFromFunction, convertToFunction);
      case FieldMergeOperator.MERGE_MAP:
        return new FieldMergeOperator.MergeMap(type, convertFromFunction, convertToFunction);
      default:
        throw new UnsupportedOperationException("Unsupported merge function:" + name);
    }
  }

  public static FieldMergeOperator[] aggregationOperators(
      Types.StructType struct,
      PrimaryKeySpec primaryKeySpec,
      Map<String, String> properties,
      BiFunction<Type, Object, Object> convertFromFunction,
      BiFunction<Type, Object, Object> convertToFunction) {

    Set<String> primaryKeyFields = Sets.newHashSet(primaryKeySpec.fieldNames());
    int fieldSize = struct.fields().size();
    FieldMergeOperator[] operators = new FieldMergeOperator[fieldSize];
    for (int i = 0; i < fieldSize; i++) {
      Types.NestedField field = struct.fields().get(i);
      // Ignore primary key fields
      if (!primaryKeyFields.contains(field.name())) {
        String fieldAggregationProperty = fieldAggregationProperty(field.name());
        if (properties.containsKey(fieldAggregationProperty)) {
          operators[i] =
              operator(
                  properties.get(fieldAggregationProperty),
                  field.type(),
                  convertFromFunction,
                  convertToFunction);
        } else {
          operators[i] = FieldMergeOperator.Last.get();
        }
      } else {
        operators[i] = FieldMergeOperator.Last.get();
      }
    }

    return operators;
  }

  public static String fieldAggregationProperty(String fieldName) {
    return "fields." + fieldName + ".aggregate-function";
  }

  public static FieldMergeOperator[] partialUpdateOperators(Types.StructType struct) {

    int fieldSize = struct.fields().size();
    FieldMergeOperator[] operators = new FieldMergeOperator[fieldSize];
    for (int i = 0; i < fieldSize; i++) {
      operators[i] = FieldMergeOperator.LastNonNull.get();
    }

    return operators;
  }

  public static boolean supportAggregationFunction(Type type, String name) {
    switch (name) {
      case FieldMergeOperator.LAST:
      case FieldMergeOperator.LAST_NON_NULL:
      case FieldMergeOperator.FIRST:
      case FieldMergeOperator.FIRST_NON_NULL:
        return true;
      case FieldMergeOperator.SUM:
      case FieldMergeOperator.PRODUCT:
        return FieldMergeOperator.NUMBER_TYPES.contains(type.typeId());
      case FieldMergeOperator.MAX:
      case FieldMergeOperator.MIN:
        return FieldMergeOperator.COMPARABLE_TYPES.contains(type.typeId());
      case FieldMergeOperator.BOOL_AND:
      case FieldMergeOperator.BOOL_OR:
        return Type.TypeID.BOOLEAN.equals(type.typeId());
      case FieldMergeOperator.MERGE_MAP:
        return Type.TypeID.MAP.equals(type.typeId());
      default:
        return false;
    }
  }
}
