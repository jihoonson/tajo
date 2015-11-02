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
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.BytesUtils;
import org.apache.tajo.util.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

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
   * @param sortSpec
   * @param range
   * @param i
   * @return
   */
  public static BigInteger computeCardinality(SortSpec sortSpec, TupleRange range, int i,
                                              boolean lastInclusive) {
    return computeCardinality(sortSpec, range.getStart(), range.getEnd(), range.getBase(), i, lastInclusive);
  }

  public static BigInteger computeCardinality(SortSpec sortSpec, Tuple start, Tuple end, Tuple base, int i,
                                              boolean lastInclusive) {
    BigInteger columnCard;
    DataType dataType = sortSpec.getSortKey().getDataType();

    switch (dataType.getType()) {
      case BOOLEAN:
        columnCard = BigInteger.valueOf(2);
        break;
      case CHAR:
        // TODO: round
        char startChar = start.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Character.MAX_VALUE) : start.getChar(i);
        char endChar = end.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Character.MAX_VALUE) : end.getChar(i);

        if (!sortSpec.isAscending()) {
          char tmp = startChar;
          startChar = endChar;
          endChar = tmp;
        }
        columnCard = BigInteger.valueOf((endChar - startChar) / base.getChar(i));
        break;
//      case BIT:
//        byte startByte = start.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : 0x) : start.getByte(i);
//
//        if (sortSpec.isAscending()) {
//          columnCard = BigInteger.valueOf((end.getByte(i) - start.getByte(i)) / base.getByte(i));
//        } else {
//          columnCard = BigInteger.valueOf((start.getByte(i) - end.getByte(i)) / base.getByte(i));
//        }
//        break;
      case INT2:
        short startShort = start.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Short.MAX_VALUE) : start.getInt2(i);
        short endShort = end.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Short.MAX_VALUE) : end.getInt2(i);

        if (!sortSpec.isAscending()) {
          short tmp = startShort;
          startShort = endShort;
          endShort = tmp;
        }

        columnCard = BigInteger.valueOf((endShort - startShort) / base.getInt2(i));
        break;
      case INT4:
      case DATE:
      case INET4:
        int startInt = start.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Integer.MAX_VALUE) : start.getInt4(i);
        int endInt = end.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Integer.MAX_VALUE) : end.getInt4(i);

        if (!sortSpec.isAscending()) {
          int tmp = startInt;
          startInt = endInt;
          endInt = tmp;
        }

        columnCard = BigInteger.valueOf((endInt - startInt) / base.getInt4(i));
        break;
      case INT8:
      case TIME:
      case TIMESTAMP:
        long startLong = start.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Long.MAX_VALUE) : start.getInt8(i);
        long endLong = end.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Long.MAX_VALUE) : end.getInt8(i);

        if (!sortSpec.isAscending()) {
          long tmp = startLong;
          startLong = endLong;
          endLong = tmp;
        }

        columnCard = BigInteger.valueOf((endLong - startLong) / base.getInt8(i));
        break;
      case FLOAT4:
        float startFloat = start.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Float.MAX_VALUE) : start.getFloat4(i);
        float endFloat = end.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Float.MAX_VALUE) : end.getFloat4(i);

        if (!sortSpec.isAscending()) {
          float tmp = startFloat;
          startFloat = endFloat;
          endFloat = tmp;
        }

        // TODO: round
        columnCard = BigDecimal.valueOf(endFloat).subtract(BigDecimal.valueOf(startFloat)).divide(BigDecimal.valueOf(base.getFloat4(i)), MathContext.DECIMAL128).toBigInteger();
        break;
      case FLOAT8:
        double startDouble = start.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Double.MAX_VALUE) : start.getFloat8(i);
        double endDouble = end.isBlankOrNull(i) ? (sortSpec.isNullFirst() ? 0 : Double.MAX_VALUE) : end.getFloat8(i);

        if (!sortSpec.isAscending()) {
          double tmp = startDouble;
          startDouble = endDouble;
          endDouble = tmp;
        }

        columnCard = BigDecimal.valueOf(endDouble).subtract(BigDecimal.valueOf(startDouble)).divide(BigDecimal.valueOf(base.getFloat8(i)), MathContext.DECIMAL128).toBigInteger();
        break;
      case TEXT: {
        boolean isPureAscii = StringUtils.isPureAscii(start.getText(i)) && StringUtils.isPureAscii(end.getText(i));

        if (isPureAscii) {
          byte[] s;
          byte[] e;
          byte[] b = base.getBytes(i);
          if (sortSpec.isAscending()) {
            s = start.getBytes(i);
            e = end.getBytes(i);
          } else {
            e = start.getBytes(i);
            s = end.getBytes(i);
          }

          byte [][] padded = BytesUtils.padBytes(s, e);
          s = padded[0];
          e = padded[1];

          byte[] prependHeader = {1, 0};
          final BigInteger startBI = new BigInteger(Bytes.add(prependHeader, s));
          final BigInteger stopBI = new BigInteger(Bytes.add(prependHeader, e));

          final BigInteger baseBI = new BigInteger(b);
          columnCard = stopBI.subtract(startBI).divide(baseBI);
        } else {
          char [] s;
          char [] e;
          char [] b = base.getUnicodeChars(i);

          if (sortSpec.isAscending()) {
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
      columnCard = computeCardinality(sortSpecs[i], start, end, base, i, lastInclusive);

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
