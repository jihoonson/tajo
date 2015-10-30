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
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.BytesUtils;
import org.apache.tajo.util.StringUtils;

import java.math.BigDecimal;
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
   * It computes the value cardinality of a tuple range.
   *
   * @param dataType
   * @param range
   * @param i
   * @return
   */
  public static BigInteger computeCardinality(DataType dataType, TupleRange range, int i,
                                              boolean lastInclusive, boolean isAscending) {
    return computeCardinality(dataType, range.getStart(), range.getEnd(), range.getBase(), i, lastInclusive,
        isAscending);
  }

  public static BigInteger computeCardinality(DataType dataType, Tuple start, Tuple end, Tuple base, int i,
                                              boolean lastInclusive, boolean isAscending) {
    BigInteger columnCard;


    switch (dataType.getType()) {
      case BOOLEAN:
        columnCard = BigInteger.valueOf(2);
        break;
      case CHAR:
        if (isAscending) {
          columnCard = BigInteger.valueOf((end.getChar(i) - start.getChar(i)) / base.getChar(i));
        } else {
          columnCard = BigInteger.valueOf((start.getChar(i) - end.getChar(i)) / base.getChar(i));
        }
        break;
      case BIT:
        if (isAscending) {
          columnCard = BigInteger.valueOf((end.getByte(i) - start.getByte(i)) / base.getByte(i));
        } else {
          columnCard = BigInteger.valueOf((start.getByte(i) - end.getByte(i)) / base.getByte(i));
        }
        break;
      case INT2:
        if (isAscending) {
          columnCard = BigInteger.valueOf((end.getInt2(i) - start.getInt2(i)) / base.getInt2(i));
        } else {
          columnCard = BigInteger.valueOf((start.getInt2(i) - end.getInt2(i)) / base.getInt2(i));
        }
        break;
      case INT4:
      case DATE:
      case INET4:
        if (isAscending) {
          columnCard = BigInteger.valueOf((end.getInt4(i) - start.getInt4(i)) / base.getInt4(i));
        } else {
          columnCard = BigInteger.valueOf((start.getInt4(i) - end.getInt4(i)) / base.getInt4(i));
        }
        break;
      case INT8:
      case TIME:
      case TIMESTAMP:
        if (isAscending) {
          columnCard = BigInteger.valueOf((end.getInt8(i) - start.getInt8(i)) / base.getInt8(i));
        } else {
          columnCard = BigInteger.valueOf((start.getInt8(i) - end.getInt8(i)) / base.getInt8(i));
        }
        break;
      case FLOAT4:
        if (isAscending) {
//          columnCard = BigInteger.valueOf(end.getInt4(i) - start.getInt4(i));
          // TODO: round
          columnCard = BigDecimal.valueOf(end.getFloat4(i)).subtract(BigDecimal.valueOf(start.getFloat4(i))).divide(BigDecimal.valueOf(base.getFloat4(i))).toBigInteger();
        } else {
//          columnCard = BigInteger.valueOf(start.getInt4(i) - end.getInt4(i));
          columnCard = BigDecimal.valueOf(start.getFloat4(i)).subtract(BigDecimal.valueOf(end.getFloat4(i))).divide(BigDecimal.valueOf(base.getFloat4(i))).toBigInteger();
        }
        break;
      case FLOAT8:
        if (isAscending) {
//          columnCard = BigInteger.valueOf(end.getInt8(i) - start.getInt8(i));
          columnCard = BigDecimal.valueOf(end.getFloat8(i)).subtract(BigDecimal.valueOf(start.getFloat8(i))).divide(BigDecimal.valueOf(base.getFloat8(i))).toBigInteger();
        } else {
//          columnCard = BigInteger.valueOf(start.getInt8(i) - end.getInt8(i));
          columnCard = BigDecimal.valueOf(start.getFloat8(i)).subtract(BigDecimal.valueOf(end.getFloat8(i))).divide(BigDecimal.valueOf(base.getFloat8(i))).toBigInteger();
        }
        break;
      case TEXT: {
        boolean isPureAscii = StringUtils.isPureAscii(start.getText(i)) && StringUtils.isPureAscii(end.getText(i));

        if (isPureAscii) {
          byte[] s;
          byte[] e;
          byte[] b = base.getBytes(i);
          if (isAscending) {
            s = start.getBytes(i);
            e = end.getBytes(i);
          } else {
            e = start.getBytes(i);
            s = end.getBytes(i);
          }

          byte [][] padded = BytesUtils.padBytes(s, e);
          s = padded[0];
          e = padded[1];
//          b = padded[2];

          byte[] prependHeader = {1, 0};
          final BigInteger startBI = new BigInteger(Bytes.add(prependHeader, s));
          final BigInteger stopBI = new BigInteger(Bytes.add(prependHeader, e));
//          final BigInteger baseBI = new BigInteger(Bytes.add(prependHeader, b));

          final BigInteger baseBI = new BigInteger(b);
          columnCard = stopBI.subtract(startBI).divide(baseBI);
        } else {
          char [] s;
          char [] e;
          char [] b = base.getUnicodeChars(i);

          if (isAscending) {
            s = start.getUnicodeChars(i);
            e = end.getUnicodeChars(i);
          } else {
            e = start.getUnicodeChars(i);
            s = end.getUnicodeChars(i);
          }

          BigInteger startBI = charsToBigInteger(s);
          BigInteger stopBI = charsToBigInteger(e);
          BigInteger baseBI = charsToBigInteger(b);

          columnCard = stopBI.subtract(startBI).divide(baseBI);
        }
        break;
      }
      default:
        throw new UnsupportedOperationException(dataType + " is not supported yet");
    }

    return lastInclusive ? columnCard.add(BigInteger.valueOf(1)).abs() : columnCard.abs();
  }

  public static BigInteger charsToBigInteger(char [] chars) {
    BigInteger digitBase;
    BigInteger sum = BigInteger.ZERO;
    for (int i = chars.length - 1; i >= 0; i--) {
      BigInteger charVal = BigInteger.valueOf(chars[(chars.length - 1) - i]);
      if (i > 0) {
        digitBase = charVal.multiply(BigInteger.valueOf(TextDatum.UNICODE_CHAR_BITS_NUM).pow(i));
        sum = sum.add(digitBase);
      } else {
        sum = sum.add(charVal);
      }
    }
    return sum;
  }

  public static BigInteger computeCardinalityForAllColumns(SortSpec[] sortSpecs, Tuple start, Tuple end, Tuple base,
                                                           boolean lastInclusive) {
    BigInteger cardinality = BigInteger.ONE;
    BigInteger columnCard;
    for (int i = 0; i < sortSpecs.length; i++) {
      columnCard = computeCardinality(sortSpecs[i].getSortKey().getDataType(), start, end, base, i, lastInclusive,
          sortSpecs[i].isAscending());

      if (BigInteger.ZERO.compareTo(columnCard) < 0) {
        cardinality = cardinality.multiply(columnCard);
      }
    }

    return cardinality;
  }

  /**
   * It computes the value cardinality of a tuple range.
   * @return
   */
  public static BigInteger computeCardinalityForAllColumns(SortSpec[] sortSpecs, TupleRange range, boolean lastInclusive) {
    return computeCardinalityForAllColumns(sortSpecs, range.getStart(), range.getEnd(), range.getBase(), lastInclusive);
  }
}
