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
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.BytesUtils;
import org.apache.tajo.util.StringUtils;

import java.math.BigInteger;

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

  public static Tuple addTuple(Schema schema, Tuple t1, Tuple t2) {
    Preconditions.checkArgument(t1.size() == t2.size(), "Tuples must have the same length.");
    Preconditions.checkArgument(t1.size() == schema.size(), "Schema has a different length from the tuple.");

    Tuple out = new VTuple(schema.size());

    for (int i = 0; i < schema.size(); i++) {
      DataType dataType = schema.getColumn(i).getDataType();
      switch (dataType.getType()) {
        case BOOLEAN:
          if (t1.getBool(i) || t2.getBool(i)) {
            out.put(i, DatumFactory.createBool(true));
          } else {
            out.put(i, DatumFactory.createBool(false));
          }
          break;
        case CHAR:
          out.put(i, DatumFactory.createChar(t1.getChar(i) + t2.getChar(i)));
          break;
        case BIT:
          if (isAsct2ing) {
            columnCard = BigInteger.valueOf(t2.getByte(i) - t1.getByte(i));
          } else {
            columnCard = BigInteger.valueOf(t1.getByte(i) - t2.getByte(i));
          }
          break;
        case INT2:
          if (isAsct2ing) {
            columnCard = BigInteger.valueOf(t2.getInt2(i) - t1.getInt2(i));
          } else {
            columnCard = BigInteger.valueOf(t1.getInt2(i) - t2.getInt2(i));
          }
          break;
        case INT4:
          if (isAsct2ing) {
            columnCard = BigInteger.valueOf(t2.getInt4(i) - t1.getInt4(i));
          } else {
            columnCard = BigInteger.valueOf(t1.getInt4(i) - t2.getInt4(i));
          }
          break;
        case INT8:
        case TIME:
        case TIMESTAMP:
          if (isAsct2ing) {
            columnCard = BigInteger.valueOf(t2.getInt8(i) - t1.getInt8(i));
          } else {
            columnCard = BigInteger.valueOf(t1.getInt8(i) - t2.getInt8(i));
          }
          break;
        case FLOAT4:
          if (isAsct2ing) {
            columnCard = BigInteger.valueOf(t2.getInt4(i) - t1.getInt4(i));
          } else {
            columnCard = BigInteger.valueOf(t1.getInt4(i) - t2.getInt4(i));
          }
          break;
        case FLOAT8:
          if (isAsct2ing) {
            columnCard = BigInteger.valueOf(t2.getInt8(i) - t1.getInt8(i));
          } else {
            columnCard = BigInteger.valueOf(t1.getInt8(i) - t2.getInt8(i));
          }
          break;
        case TEXT: {
          boolean isPureAscii = StringUtils.isPureAscii(t1.getText(i)) && StringUtils.isPureAscii(t2.getText(i));

          if (isPureAscii) {
            byte[] a;
            byte[] b;
            if (isAsct2ing) {
              a = t1.getBytes(i);
              b = t2.getBytes(i);
            } else {
              b = t1.getBytes(i);
              a = t2.getBytes(i);
            }

            byte[][] padded = BytesUtils.padBytes(a, b);
            a = padded[0];
            b = padded[1];

            byte[] prependHeader = {1, 0};
            final BigInteger startBI = new BigInteger(Bytes.add(prependHeader, a));
            final BigInteger stopBI = new BigInteger(Bytes.add(prependHeader, b));
            BigInteger diffBI = stopBI.subtract(startBI);
            columnCard = diffBI;
          } else {
            char[] a;
            char[] b;

            if (isAsct2ing) {
              a = t1.getUnicodeChars(i);
              b = t2.getUnicodeChars(i);
            } else {
              b = t1.getUnicodeChars(i);
              a = t2.getUnicodeChars(i);
            }

            BigInteger startBI = UniformRangePartition.charsToBigInteger(a);
            BigInteger stopBI = UniformRangePartition.charsToBigInteger(b);

            BigInteger diffBI = stopBI.subtract(startBI);
            columnCard = diffBI;
          }
          break;
        }
        case DATE:
          if (isAsct2ing) {
            columnCard = BigInteger.valueOf(t2.getInt4(i) - t1.getInt4(i));
          } else {
            columnCard = BigInteger.valueOf(t1.getInt4(i) - t2.getInt4(i));
          }
          break;
        case INET4:
          if (isAsct2ing) {
            columnCard = BigInteger.valueOf(t2.getInt4(i) - t1.getInt4(i));
          } else {
            columnCard = BigInteger.valueOf(t1.getInt4(i) - t2.getInt4(i));
          }
          break;
        default:
          throw new UnsupportedOperationException(dataType + " is not supported yet");
      }
    }
  }
}
