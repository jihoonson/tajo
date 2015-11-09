/**
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

package org.apache.tajo.catalog;

import com.google.common.base.Preconditions;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.StorageProtos.TupleComparatorProto;
import org.apache.tajo.storage.Tuple;

import java.util.Arrays;


/**
 * The Comparator class for Tuples
 * 
 * @see Tuple
 */
public class BaseTupleComparator extends TupleComparator implements ProtoObject<TupleComparatorProto> {
  private final Schema schema; // entire schema
  private final SortSpec [] sortSpecs; // sort specifications. 
  private final int[] sortKeyIds;

  private Datum left;
  private Datum right;
  private int compVal;

  /**
   * @param schema The schema of input tuples
   * @param sortKeys The description of sort keys
   */
  public BaseTupleComparator(Schema schema, SortSpec[] sortKeys) {
    Preconditions.checkArgument(sortKeys.length > 0, 
        "At least one sort key must be specified.");

    this.schema = schema;
    this.sortSpecs = sortKeys;
    this.sortKeyIds = new int[sortKeys.length];
    for (int i = 0; i < sortKeys.length; i++) {
      if (sortKeys[i].getSortKey().hasQualifier()) {
        this.sortKeyIds[i] = schema.getColumnId(sortKeys[i].getSortKey().getQualifiedName());
      } else {
        this.sortKeyIds[i] = schema.getColumnIdByName(sortKeys[i].getSortKey().getSimpleName());
      }
    }
  }

  public BaseTupleComparator(TupleComparatorProto proto) {
    this.schema = new Schema(proto.getSchema());

    this.sortSpecs = new SortSpec[proto.getSortSpecsCount()];
    for (int i = 0; i < proto.getSortSpecsCount(); i++) {
      sortSpecs[i] = new SortSpec(proto.getSortSpecs(i));
    }

    this.sortKeyIds = new int[sortSpecs.length];
    for (int i = 0; i < sortSpecs.length; i++) {
      if (sortSpecs[i].getSortKey().hasQualifier()) {
        this.sortKeyIds[i] = schema.getColumnId(sortSpecs[i].getSortKey().getQualifiedName());
      } else {
        this.sortKeyIds[i] = schema.getColumnIdByName(sortSpecs[i].getSortKey().getSimpleName());
      }
    }
  }

  public Schema getSchema() {
    return schema;
  }

  public SortSpec [] getSortSpecs() {
    return sortSpecs;
  }

  public int [] getSortKeyIds() {
    return sortKeyIds;
  }

  @Override
  public boolean isAscendingFirstKey() {
    return this.sortSpecs[0].isAscending();
  }

  @Override
  public int compare(Tuple tuple1, Tuple tuple2) {
    for (int i = 0; i < sortKeyIds.length; i++) {
      left = tuple1.asDatum(sortKeyIds[i]);
      right = tuple2.asDatum(sortKeyIds[i]);

      if (left.isNull() || right.isNull()) {
        if (!left.equals(right)) {
          if (left.isNull()) {
            compVal = sortSpecs[i].isNullFirst() ? -1 : 1;
          } else {
            compVal = sortSpecs[i].isNullFirst() ? 1 : -1;
          }
        } else {
          compVal = 0;
        }
      } else {
        if (sortSpecs[i].isAscending()) {
          compVal = left.compareTo(right);
        } else {
          compVal = right.compareTo(left);
        }
      }

      if (compVal < 0 || compVal > 0) {
        return compVal;
      }
    }
    return 0;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(sortSpecs);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BaseTupleComparator) {
      BaseTupleComparator other = (BaseTupleComparator) obj;
      if (sortKeyIds.length != other.sortKeyIds.length) {
        return false;
      }

      for (int i = 0; i < sortKeyIds.length; i++) {
        if (sortKeyIds[i] != other.sortKeyIds[i] ||
            sortSpecs[i].isAscending() != other.sortSpecs[i].isAscending() ||
            sortSpecs[i].isNullFirst() != other.sortSpecs[i].isNullFirst()) {
          return false;
        }
      }

      return true;
    } else {
      return false;
    }
  }

  @Override
  public TupleComparatorProto getProto() {
    TupleComparatorProto.Builder builder = TupleComparatorProto.newBuilder();
    builder.setSchema(schema.getProto());
    for (int i = 0; i < sortSpecs.length; i++) {
      builder.addSortSpecs(sortSpecs[i].getProto());
    }

    return builder.build();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    String prefix = "";
    for (int i = 0; i < sortKeyIds.length; i++) {
      sb.append(prefix).append("SortKeyId=").append(sortKeyIds[i])
        .append(",Asc=").append(sortSpecs[i].isAscending())
        .append(",NullFirst=").append(sortSpecs[i].isNullFirst());
      prefix = " ,";
    }
    return sb.toString();
  }
}