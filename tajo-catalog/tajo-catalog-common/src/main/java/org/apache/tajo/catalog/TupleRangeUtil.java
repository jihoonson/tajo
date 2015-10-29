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

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

public class TupleRangeUtil {

  public static Datum minBase(DataType dataType) {
    switch (dataType.getType()) {
      case NULL_TYPE:
        return NullDatum.get();
      case BOOLEAN:
      case BIT:
        return DatumFactory.createBit((byte) 1);
      case INT1:
      case INT2:
        return DatumFactory.createInt2((short) 1);
      case INT4:
        return DatumFactory.createInt4(1);
      case INT8:
        return DatumFactory.createInt8(1);
      case FLOAT4:
        return DatumFactory.createFloat4(1.f);
      case FLOAT8:
        return DatumFactory.createFloat8(1.d);
      case CHAR:
        return DatumFactory.createChar((byte) 1);
      case TEXT: {
        byte[] bytes = new byte[1];
        bytes[0] = 1;
        return DatumFactory.createText(bytes);
      }
      case DATE:
        return DatumFactory.createDate(1);
      case TIME:
        return DatumFactory.createTime(1);
      case TIMESTAMP:
        return DatumFactory.createTimestamp(1);
      case INTERVAL:
        return DatumFactory.createInterval(1);
      case BLOB: {
        byte[] bytes = new byte[1];
        bytes[0] = 1;
        return DatumFactory.createBlob(bytes);
      }
      case INET4:
        return DatumFactory.createInet4(1);
      default:
        throw new TajoInternalError(new NotImplementedException(dataType.getType().name()));
    }
  }

  public static Tuple createMinBaseTuple(SortSpec[] sortSpecs) {
    Tuple base = new VTuple(sortSpecs.length);
    for (int i = 0; i < sortSpecs.length; i++) {
      base.put(i, minBase(sortSpecs[i].getSortKey().getDataType()));
    }
    return base;
  }

  /**
   *
   * @param t1
   * @param t2
   * @return
   */
  public static Tuple diff(TupleRange t1, TupleRange t2) {
    if (t1.compareTo(t2) < 0) {

    }
    return null;
  }
}
