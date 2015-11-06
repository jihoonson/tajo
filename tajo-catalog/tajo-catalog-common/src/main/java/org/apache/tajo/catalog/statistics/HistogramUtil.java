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

package org.apache.tajo.catalog.statistics;

import com.sun.tools.javac.util.Convert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.FreqHistogram.Bucket;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.datetime.DateTimeUtil;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class HistogramUtil {

  private static final Log LOG = LogFactory.getLog(HistogramUtil.class);

//  public final static MathContext MATH_CONTEXT = new MathContext(50, RoundingMode.HALF_DOWN);

//  /**
//   * Normalize ranges of buckets into the range of [0, 1).
//   *
//   * @param histogram
//   * @param totalRange
//   * @return
//   */
//  public static FreqHistogram normalize(final FreqHistogram histogram,
//                                        final TupleRange totalRange,
//                                        final BigInteger totalCardinality) {
//    // TODO: Type must be able to contain BigDecimal
//    Schema normalizedSchema = new Schema(new Column[]{new Column("normalized", Type.FLOAT8)});
//    SortSpec[] normalizedSortSpecs = new SortSpec[1];
//    normalizedSortSpecs[0] = new SortSpec(normalizedSchema.getColumn(0), true, false);
//    FreqHistogram normalized = new FreqHistogram(normalizedSchema, normalizedSortSpecs);
//
//    // TODO: calculate diff instead of cardinality
//    BigDecimal totalDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(histogram.sortSpecs,
//        totalRange, true)).multiply(new BigDecimal(totalCardinality));
//    BigDecimal baseDiff = BigDecimal.ONE;
//
//    Tuple normalizedBase = new VTuple(normalizedSchema.size());
//    BigDecimal div = baseDiff.divide(totalDiff, MathContext.DECIMAL128);
//    normalizedBase.put(0, DatumFactory.createFloat8(div.doubleValue()));
//
//    for (Bucket eachBucket: histogram.buckets.values()) {
//      BigDecimal startDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(histogram.sortSpecs,
//          totalRange.getStart(), eachBucket.getStartKey(), totalRange.getBase(), true).subtract(BigInteger.ONE));
//      BigDecimal endDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(histogram.sortSpecs,
//          totalRange.getStart(), eachBucket.getEndKey(), totalRange.getBase(), true).subtract(BigInteger.ONE));
//      Tuple normalizedStartTuple = new VTuple(
//          new Datum[]{
//              DatumFactory.createFloat8(startDiff.divide(totalDiff, MathContext.DECIMAL128).doubleValue())
//          });
//      Tuple normalizedEndTuple = new VTuple(
//          new Datum[]{
//              DatumFactory.createFloat8(endDiff.divide(totalDiff, MathContext.DECIMAL128).doubleValue())
//          });
//      normalized.updateBucket(normalizedStartTuple, normalizedEndTuple, normalizedBase, eachBucket.getCount());
//    }
//
//    return normalized;
//  }
//
//  /**
//   *
//   * @param normalized
//   * @param denormalizedSchema
//   * @param denormalizedSortSpecs
//   * @param columnStatsList
//   * @param totalRange
//   * @return
//   */
//  public static FreqHistogram denormalize(FreqHistogram normalized,
//                                          Schema denormalizedSchema,
//                                          SortSpec[] denormalizedSortSpecs,
//                                          List<ColumnStats> columnStatsList,
//                                          TupleRange totalRange) {
//    FreqHistogram denormalized = new FreqHistogram(denormalizedSchema, denormalizedSortSpecs);
//    Tuple start = new VTuple(totalRange.getStart());
//    List<Bucket> buckets = normalized.getSortedBuckets();
//
//    for (Bucket eachBucket: buckets) {
//      Tuple end = increment(denormalizedSortSpecs, columnStatsList, start, totalRange.getBase(), eachBucket.getCount());
//      denormalized.updateBucket(start, end, totalRange.getBase(), eachBucket.getCount());
//      start = end;
//    }
//    return denormalized;
//  }

  private static BigDecimal[] getNormalizedMinMax(final DataType dataType,
                                                @Nullable final ColumnStats columnStats,
                                                final boolean isPureAscii,
                                                final int textLength) {
    BigDecimal min, max;
    // TODO: min value may not be zero
    switch (dataType.getType()) {
      case BOOLEAN:
        max = BigDecimal.ONE;
        min = BigDecimal.ZERO;
        break;
      case INT2:
        if (columnStats != null) {
          max = BigDecimal.valueOf(columnStats.getMaxValue().asInt2());
          min = BigDecimal.valueOf(columnStats.getMinValue().asInt2());
        } else {
          max = BigDecimal.valueOf(Short.MAX_VALUE);
          min = BigDecimal.ZERO;
        }
        break;
      case INT4:
        if (columnStats != null) {
          max = BigDecimal.valueOf(columnStats.getMaxValue().asInt4());
          min = BigDecimal.valueOf(columnStats.getMinValue().asInt4());
        } else {
          max = BigDecimal.valueOf(Integer.MAX_VALUE);
          min = BigDecimal.ZERO;
        }
        break;
      case INT8:
        if (columnStats != null) {
          max = BigDecimal.valueOf(columnStats.getMaxValue().asInt8());
          min = BigDecimal.valueOf(columnStats.getMinValue().asInt8());
        } else {
          max = BigDecimal.valueOf(Long.MAX_VALUE);
          min = BigDecimal.ZERO;
        }
        break;
      case FLOAT4:
        if (columnStats != null) {
          max = BigDecimal.valueOf(columnStats.getMaxValue().asFloat4());
          min = BigDecimal.valueOf(columnStats.getMinValue().asFloat4());
        } else {
          max = BigDecimal.valueOf(Float.MAX_VALUE);
          min = BigDecimal.ZERO;
        }
        break;
      case FLOAT8:
        if (columnStats != null) {
          max = BigDecimal.valueOf(columnStats.getMaxValue().asFloat8());
          min = BigDecimal.valueOf(columnStats.getMinValue().asFloat8());
        } else {
          max = BigDecimal.valueOf(Double.MAX_VALUE);
          min = BigDecimal.ZERO;
        }
        break;
      case CHAR:
        if (columnStats != null) {
          max = BigDecimal.valueOf(columnStats.getMaxValue().asChar());
          min = BigDecimal.valueOf(columnStats.getMinValue().asChar());
        } else {
          max = BigDecimal.valueOf(Character.MAX_VALUE);
          min = BigDecimal.ZERO;
        }
        break;
      case TEXT:
        if (isPureAscii) {
          if (columnStats != null) {
            max = new BigDecimal(new BigInteger(columnStats.getMaxValue().asByteArray()));
            min = new BigDecimal(new BigInteger(columnStats.getMinValue().asByteArray()));
          } else {
            byte[] maxBytes = new byte[textLength];
            byte[] minBytes = new byte[textLength];
            Arrays.fill(maxBytes, (byte) 127);
            Bytes.zero(minBytes);
            max = new BigDecimal(new BigInteger(maxBytes));
            min = new BigDecimal(new BigInteger(minBytes));
          }
        } else {
          if (columnStats != null) {
            max = unicodeCharsToBigDecimal(columnStats.getMaxValue().asUnicodeChars());
            min = unicodeCharsToBigDecimal(columnStats.getMinValue().asUnicodeChars());
          } else {
            char[] maxChars = new char[textLength];
            char[] minChars = new char[textLength];
            Arrays.fill(maxChars, Character.MAX_VALUE);
            Arrays.fill(minChars, Character.MIN_VALUE);
            max = unicodeCharsToBigDecimal(maxChars);
            min = unicodeCharsToBigDecimal(minChars);
          }
        }
        break;
      case DATE:
        if (columnStats != null) {
          max = BigDecimal.valueOf(columnStats.getMaxValue().asInt4());
          min = BigDecimal.valueOf(columnStats.getMinValue().asInt4());
        } else {
          max = BigDecimal.valueOf(Integer.MAX_VALUE);
          min = BigDecimal.ZERO;
        }
        break;
      case TIME:
        if (columnStats != null) {
          max = BigDecimal.valueOf(columnStats.getMaxValue().asInt8());
          min = BigDecimal.valueOf(columnStats.getMinValue().asInt8());
        } else {
          max = BigDecimal.valueOf(Long.MAX_VALUE);
          min = BigDecimal.ZERO;
        }
        break;
      case TIMESTAMP:
        if (columnStats != null) {
          max = BigDecimal.valueOf(((TimestampDatum)columnStats.getMaxValue()).getJavaTimestamp());
          min = BigDecimal.valueOf(((TimestampDatum)columnStats.getMinValue()).getJavaTimestamp());
        } else {
          max = BigDecimal.valueOf(DateTimeUtil.julianTimeToJavaTime(Long.MAX_VALUE));
          min = BigDecimal.valueOf(DateTimeUtil.julianTimeToJavaTime(0));
        }
        break;
      case INET4:
        if (columnStats != null) {
          max = BigDecimal.valueOf(columnStats.getMaxValue().asInt8());
          min = BigDecimal.valueOf(columnStats.getMinValue().asInt8());
        } else {
          max = BigDecimal.valueOf(Long.MAX_VALUE);
          min = BigDecimal.ZERO;
        }
        break;
      default:
        throw new UnsupportedOperationException(dataType.getType() + " is not supported yet");
    }
    return new BigDecimal[] {min, max};
  }

  private static Datum denormalize(final DataType dataType,
                                   final BigDecimal val,
                                   final boolean isPureAscii) {
    switch (dataType.getType()) {
      case BOOLEAN:
        return val.longValue() % 2 == 0 ? BooleanDatum.FALSE : BooleanDatum.TRUE;
      case CHAR:
        return DatumFactory.createChar((char) val.longValue());
      case INT2:
        return DatumFactory.createInt2(val.shortValue());
      case INT4:
        return DatumFactory.createInt4(val.intValue());
      case INT8:
        return DatumFactory.createInt8(val.longValue());
      case FLOAT4:
        return DatumFactory.createFloat4(val.floatValue());
      case FLOAT8:
        return DatumFactory.createFloat8(val.doubleValue());
      case TEXT:
        if (isPureAscii) {
          return DatumFactory.createText(val.toBigInteger().toByteArray());
        } else {
          return DatumFactory.createText(Convert.chars2utf(bigDecimalToUnicodeChars(val)));
        }
      case DATE:
        return DatumFactory.createDate(val.intValue());
      case TIME:
        return DatumFactory.createTime(val.longValue());
      case TIMESTAMP:
        return DatumFactory.createTimestampDatumWithJavaMillis(val.longValue());
      case INET4:
        return DatumFactory.createInet4((int) val.longValue());
      default:
        throw new UnsupportedOperationException(dataType.getType() + " is not supported yet");
    }
  }

  private static BigDecimal normalize(final DataType dataType,
                                      final Datum val,
                                      final boolean isPureAscii) {
    switch (dataType.getType()) {
      case CHAR:
        return BigDecimal.valueOf(val.asChar());
      case INT2:
        return BigDecimal.valueOf(val.asInt2());
      case INT4:
        return BigDecimal.valueOf(val.asInt4());
      case INT8:
        return BigDecimal.valueOf(val.asInt8());
      case FLOAT4:
        return BigDecimal.valueOf(val.asFloat4());
      case FLOAT8:
        return BigDecimal.valueOf(val.asFloat8());
      case TEXT:
        if (isPureAscii) {
          return new BigDecimal(new BigInteger(val.asByteArray()));
        } else {
          return unicodeCharsToBigDecimal(val.asUnicodeChars());
        }
      case DATE:
        return BigDecimal.valueOf(val.asInt4());
      case TIME:
        return BigDecimal.valueOf(val.asInt8());
      case TIMESTAMP:
        return BigDecimal.valueOf(((TimestampDatum)val).getJavaTimestamp());
      case INET4:
        return BigDecimal.valueOf(val.asInt8());
      default:
        throw new UnsupportedOperationException(dataType + " is not supported yet");
    }
  }

  /**
   * Increment the tuple with the amount of the product of <code>count</code> and <code>baseTuple</code>.
   * If the <code>count</code> is a negative value, it will work as decrement.
   *
   * @param sortSpecs an array of sort specifications
   * @param columnStatsList a list of statistics which must include min-max values of each column
   * @param operand tuple to be incremented
   * @param baseTuple base tuple
   * @param count increment count. If this value is negative, this method works as decrement.
   * @return incremented tuple
   */
  public static Tuple increment(final SortSpec[] sortSpecs, @Nullable final List<ColumnStats> columnStatsList,
                                final Tuple operand, final Tuple baseTuple, long count) {
    // TODO: handle infinite datums
    // TODO: handle null datums
    Tuple result = new VTuple(sortSpecs.length);
    BigDecimal add, temp;
    BigDecimal[] minMax;
    BigDecimal[] carryAndRemainder = new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ZERO};

    for (int i = sortSpecs.length-1; i >= 0; i--) {
      Column column = sortSpecs[i].getSortKey();
      boolean isPureAscii = false;

      if (operand.isBlankOrNull(i)) {
        if (columnStatsList != null) {
          result.put(i, columnStatsList.get(i).getMinValue());
        } else {
          if (column.getDataType().getType().equals(Type.TEXT)) {
            isPureAscii = StringUtils.isPureAscii(baseTuple.getText(i)) &&
                StringUtils.isPureAscii(operand.getText(i));
            minMax = getNormalizedMinMax(column.getDataType(),
                null, isPureAscii,
                baseTuple.getBytes(i).length > operand.getBytes(i).length ?
                    baseTuple.getBytes(i).length : operand.getBytes(i).length);
          } else {
            minMax = getNormalizedMinMax(column.getDataType(), null, false, 0);
          }
          result.put(i, denormalize(column.getDataType(), minMax[0], isPureAscii));
        }
        continue;
      }

      if (column.getDataType().getType().equals(Type.TEXT)) {
        isPureAscii = StringUtils.isPureAscii(baseTuple.getText(i)) &&
            StringUtils.isPureAscii(operand.getText(i));
        minMax = getNormalizedMinMax(column.getDataType(),
            columnStatsList == null ? null : columnStatsList.get(i), isPureAscii,
            baseTuple.getBytes(i).length > operand.getBytes(i).length ?
                baseTuple.getBytes(i).length : operand.getBytes(i).length);
      } else {
        minMax = getNormalizedMinMax(column.getDataType(), columnStatsList == null ? null : columnStatsList.get(i),
            false, 0);
      }

      // result = carry * max + val1 + val2
      // result > max ? result = remainder; update carry;
      count *= sortSpecs[i].isAscending() ? 1 : -1;
      BigDecimal min = minMax[0];
      BigDecimal max = minMax[1];

      switch (column.getDataType().getType()) {
        case BOOLEAN:
          // add = base * count
          add = BigDecimal.valueOf(count);
          // result = carry + val + add
          temp = operand.getBool(i) ? max : min;
          temp = carryAndRemainder[0].add(temp).add(add);
          carryAndRemainder = calculateCarryAndRemainder(temp, max, min);
          break;
        case CHAR:
        case INT2:
        case INT4:
        case INT8:
        case FLOAT4:
        case FLOAT8:
        case TEXT:
        case DATE:
        case TIME:
        case TIMESTAMP:
        case INET4:
          // add = base * count
          add = normalize(column.getDataType(), baseTuple.asDatum(i), isPureAscii)
              .multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndRemainder[0]
              .add(normalize(column.getDataType(), operand.asDatum(i), isPureAscii))
              .add(add);
          carryAndRemainder = calculateCarryAndRemainder(temp, max, min);
          break;
        default:
          throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
      }

      if (carryAndRemainder[0].compareTo(BigDecimal.ZERO) > 0) {
        // null last ? carry--;
        // remainder == 0 ? put null
        if (!sortSpecs[i].isNullFirst()) {
          carryAndRemainder[0] = carryAndRemainder[0].subtract(BigDecimal.ONE);
        }
        if (carryAndRemainder[1].equals(BigDecimal.ZERO)) {
          result.put(i, NullDatum.get());
        }
      }

      if (result.isBlank(i)) {
        result.put(i, denormalize(column.getDataType(), carryAndRemainder[1], isPureAscii));
      }
    }

    if (!carryAndRemainder[0].equals(BigDecimal.ZERO)) {
      throw new TajoInternalError("Overflow");
    }

    return result;
  }

  public static BigDecimal unicodeCharsToBigDecimal(char[] unicodeChars) {
    BigDecimal result = BigDecimal.ZERO;
    final BigDecimal base = BigDecimal.valueOf(TextDatum.UNICODE_CHAR_BITS_NUM);
    for (int i = unicodeChars.length-1; i >= 0; i--) {
      result = result.add(BigDecimal.valueOf(unicodeChars[i]).multiply(base.pow(unicodeChars.length-1-i)));
    }
    return result;
  }

  public static char[] bigDecimalToUnicodeChars(BigDecimal val) {
    final BigDecimal base = BigDecimal.valueOf(TextDatum.UNICODE_CHAR_BITS_NUM);
    List<Character> characters = new ArrayList<>();
    BigDecimal divisor = val;
    while (divisor.compareTo(base) > 0) {
      BigDecimal[] quoteAndRemainder = divisor.divideAndRemainder(base, MathContext.DECIMAL128);
      divisor = quoteAndRemainder[0];
      characters.add(0, (char) quoteAndRemainder[1].intValue());
    }
    characters.add(0, (char) divisor.intValue());
    char[] chars = new char[characters.size()];
    for (int i = 0; i < characters.size(); i++) {
      chars[i] = characters.get(i);
    }
    return chars;
  }

  /**
   *
   * @param val
   * @param max
   * @param min
   * @return
   */
  private static BigDecimal[] calculateCarryAndRemainder(BigDecimal val, BigDecimal max, BigDecimal min) {
    // TODO: if val is negative??
    BigDecimal[] carryAndRemainder = new BigDecimal[2];
    if (val.compareTo(max) > 0) {
      // quote = (max - min) / (val - min)
      // remainder = ((max - min) % (val - min)) + min
      BigDecimal[] quotAndRem = max.subtract(min).divideAndRemainder(val.subtract(min), MathContext.DECIMAL128);
      carryAndRemainder[0] = quotAndRem[0]; // set carry as quotient
      carryAndRemainder[1] = quotAndRem[1].add(min);
    } else if (val.compareTo(min) < 0) {
      // quote = ceil( (min - val) / max )
      // remainder = max * quote - (min - val)
      carryAndRemainder[0] = min.subtract(val).divide(max, BigDecimal.ROUND_CEILING);
      carryAndRemainder[1] = max.multiply(carryAndRemainder[0]).subtract(min).add(val);
    } else {
      carryAndRemainder[0] = BigDecimal.ZERO;
      carryAndRemainder[1] = val;
    }
    return carryAndRemainder;
  }

  public static void normalize(FreqHistogram histogram, List<ColumnStats> columnStatsList) {
    List<Bucket> buckets = histogram.getSortedBuckets();
    Tuple[] candidates = new Tuple[] {
        buckets.get(0).getEndKey(),
        buckets.get(buckets.size() - 1).getEndKey()
    };
    for (int i = 0; i < columnStatsList.size(); i++) {
      ColumnStats columnStats = columnStatsList.get(i);
      for (Tuple key : candidates) {
        if (key.isInfinite(i) || key.isBlankOrNull(i)) {
          continue;
        }
        switch (columnStats.getColumn().getDataType().getType()) {
          case BOOLEAN:
            // ignore
            break;
          case INT2:
            if (key.getInt2(i) > columnStats.getMaxValue().asInt2()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getInt2(i) < columnStats.getMinValue().asInt2()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case INT4:
            if (key.getInt4(i) > columnStats.getMaxValue().asInt4()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getInt4(i) < columnStats.getMinValue().asInt4()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case INT8:
            if (key.getInt8(i) > columnStats.getMaxValue().asInt8()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getInt8(i) < columnStats.getMinValue().asInt8()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case FLOAT4:
            if (key.getFloat4(i) > columnStats.getMaxValue().asFloat4()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getFloat4(i) < columnStats.getMinValue().asFloat4()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case FLOAT8:
            if (key.getFloat8(i) > columnStats.getMaxValue().asFloat8()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getFloat8(i) < columnStats.getMinValue().asFloat8()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case CHAR:
            if (key.getChar(i) > columnStats.getMaxValue().asChar()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getChar(i) < columnStats.getMinValue().asChar()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case TEXT:
            if (key.getText(i).compareTo(columnStats.getMaxValue().asChars()) > 0) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getText(i).compareTo(columnStats.getMinValue().asChars()) < 0) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case DATE:
            if (key.getInt4(i) > columnStats.getMaxValue().asInt4()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getInt4(i) < columnStats.getMinValue().asInt4()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case TIME:
            if (key.getInt8(i) > columnStats.getMaxValue().asInt8()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getInt8(i) < columnStats.getMinValue().asInt8()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case TIMESTAMP:
            if (key.getInt8(i) > columnStats.getMaxValue().asInt8()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getInt8(i) < columnStats.getMinValue().asInt8()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          case INET4:
            if (key.getInt4(i) > columnStats.getMaxValue().asInt4()) {
              key.put(i, PositiveInfiniteDatum.get());
            } else if (key.getInt4(i) < columnStats.getMinValue().asInt4()) {
              key.put(i, NegativeInfiniteDatum.get());
            }
            break;
          default:
            throw new UnsupportedOperationException(columnStats.getColumn().getDataType().getType()
                + " is not supported yet");
        }
      }
    }
  }

  /**
   * Makes the start and end keys of the TEXT type equal in length.
   *
   * @param histogram
   */
  public static void normalizeLength(FreqHistogram histogram) {
    SortSpec[] sortSpecs = histogram.sortSpecs;
    boolean needNomalize = false;
    for (SortSpec sortSpec : sortSpecs) {
      if (sortSpec.getSortKey().getDataType().getType() == TajoDataTypes.Type.TEXT) {
        needNomalize = true;
      }
    }
    if (!needNomalize) return;

    int[] maxLenOfTextCols = new int[sortSpecs.length];
    boolean[] pureAscii = new boolean[sortSpecs.length];
    Arrays.fill(pureAscii, true);

    for (Bucket bucket : histogram.getAllBuckets()) {
      for (int i = 0; i < sortSpecs.length; i++) {
        if (sortSpecs[i].getSortKey().getDataType().getType() == TajoDataTypes.Type.TEXT) {
          pureAscii[i] &= StringUtils.isPureAscii(bucket.getStartKey().getText(i)) &&
              StringUtils.isPureAscii(bucket.getEndKey().getText(i));
        }
      }
    }

    for (Bucket bucket : histogram.getAllBuckets()) {
      for (int i = 0; i < sortSpecs.length; i++) {
        if (sortSpecs[i].getSortKey().getDataType().getType() == TajoDataTypes.Type.TEXT) {
          if (pureAscii[i]) {
            if (maxLenOfTextCols[i] < bucket.getStartKey().getText(i).length()) {
              maxLenOfTextCols[i] = bucket.getStartKey().getText(i).length();
            }
            if (maxLenOfTextCols[i] < bucket.getEndKey().getText(i).length()) {
              maxLenOfTextCols[i] = bucket.getEndKey().getText(i).length();
            }
          } else {
            if (maxLenOfTextCols[i] < bucket.getStartKey().getUnicodeChars(i).length) {
              maxLenOfTextCols[i] = bucket.getStartKey().getUnicodeChars(i).length;
            }
            if (maxLenOfTextCols[i] < bucket.getEndKey().getUnicodeChars(i).length) {
              maxLenOfTextCols[i] = bucket.getEndKey().getUnicodeChars(i).length;
            }
          }
        }
      }
    }


    // normalize text fields to have same bytes length
    for (Bucket bucket : histogram.getAllBuckets()) {
      TupleRange range = bucket.getKey();
      for (int i = 0; i < sortSpecs.length; i++) {
        if (sortSpecs[i].getSortKey().getDataType().getType() == TajoDataTypes.Type.TEXT) {
          if (pureAscii[i]) {
            byte[] startBytes = null;
            byte[] endBytes = null;
            if (!range.getStart().isBlankOrNull(i)) {
              startBytes = range.getStart().getBytes(i);
            }

            if (!range.getEnd().isBlankOrNull(i)) {
              endBytes = range.getEnd().getBytes(i);
            }

            if (startBytes != null) {
              startBytes = Bytes.padTail(startBytes, maxLenOfTextCols[i] - startBytes.length);
            }
            if (endBytes != null) {
              endBytes = Bytes.padTail(endBytes, maxLenOfTextCols[i] - endBytes.length);
            }
            range.getStart().put(i, startBytes == null ? NullDatum.get() : DatumFactory.createText(startBytes));
            range.getEnd().put(i, endBytes == null ? NullDatum.get() : DatumFactory.createText(endBytes));

          } else {
            char[] startChars = null;
            char[] endChars = null;
            if (!range.getStart().isBlankOrNull(i)) {
              startChars = range.getStart().getUnicodeChars(i);
            }

            if (!range.getEnd().isBlankOrNull(i)) {
              endChars = range.getEnd().getUnicodeChars(i);
            }

            if (startChars != null) {
              startChars = StringUtils.padTail(startChars, maxLenOfTextCols[i] - startChars.length + 1);
            }
            if (endChars != null) {
              endChars = StringUtils.padTail(endChars, maxLenOfTextCols[i] - endChars.length + 1);
            }
            range.getStart().put(i, startChars == null ? NullDatum.get() :
                DatumFactory.createText(Convert.chars2utf(startChars)));
            range.getEnd().put(i, endChars == null ? NullDatum.get() :
                DatumFactory.createText(Convert.chars2utf(endChars)));
          }
        }
      }
    }
  }

  public static boolean hasInfiniteDatum(Tuple tuple) {
    for (int i = 0; i < tuple.size(); i++) {
      if (tuple.isInfinite(i)) {
        return true;
      }
    }
    return false;
  }

//  public static void refineToEquiDepth(FreqHistogram normalizedHistogram, BigInteger avgCard, Comparator<Tuple> comparator) {
  public static void refineToEquiDepth(final FreqHistogram histogram,
                                       final BigDecimal avgCard,
                                       final List<ColumnStats> columnStatsList) {
//    List<Bucket> buckets = normalizedHistogram.getSortedBuckets();
    List<Bucket> buckets = histogram.getSortedBuckets();
    Comparator<Tuple> comparator = histogram.comparator;
    SortSpec[] sortSpecs = histogram.sortSpecs;
    Bucket passed = null;

    // Refine from the last to the left direction.
    for (int i = buckets.size() - 1; i >= 0; i--) {
      Bucket current = buckets.get(i);
      // First add the passed range from the previous partition to the current one.
      if (passed != null) {
        current.merge(passed);
        passed = null;
      }

      // TODO: handle infinite in increment
      if (!hasInfiniteDatum(current.getEndKey()) &&
          !increment(sortSpecs, columnStatsList, current.getStartKey(), current.getBase(), 1).equals(current.getEndKey())) {
        LOG.info("start refine from the last");
        int compare = BigDecimal.valueOf(current.getCount()).compareTo(avgCard);
        if (compare < 0) {
          // Take the lacking range from the next partition.
          long require = avgCard.subtract(BigDecimal.valueOf(current.getCount())).round(MathContext.DECIMAL128).longValue();
          for (int j = i - 1; j >= 0 && require > 0; j--) {
            Bucket nextBucket = buckets.get(j);
            long takeAmount = require < nextBucket.getCount() ? require : nextBucket.getCount();
            Tuple newStart = increment(sortSpecs, columnStatsList, current.getStartKey(), nextBucket.getBase(), -1 * takeAmount);
            current.getKey().setStart(newStart);
            current.incCount(takeAmount);
            nextBucket.getKey().setEnd(newStart);
            nextBucket.incCount(-1 * takeAmount);
            require -= takeAmount;
          }

        } else if (compare > 0) {
          // Pass the remaining range to the next partition.
          long passAmount = BigDecimal.valueOf(current.getCount()).subtract(avgCard).round(MathContext.DECIMAL128).longValue();
          Tuple newStart = increment(sortSpecs, columnStatsList, current.getStartKey(), current.getBase(), passAmount);
          passed = histogram.createBucket(new TupleRange(current.getStartKey(), newStart, current.getBase(), comparator), passAmount);
          current.getKey().setStart(newStart);
          current.incCount(-1 * passAmount);
        }
      }
    }

    // TODO: if there are remaining passed bucket,

    // Refine from the first to the right direction
    for (int i = 0; i < buckets.size(); i++) {
      Bucket current = buckets.get(i);
      // First add the passed range from the previous partition to the current one.
      if (passed != null) {
        current.merge(passed);
        passed = null;
      }
      if (!hasInfiniteDatum(current.getEndKey()) &&
          !increment(sortSpecs, columnStatsList, current.getStartKey(), current.getBase(), 1).equals(current.getEndKey())) {
        LOG.info("start refine from the first");
        int compare = BigDecimal.valueOf(current.getCount()).compareTo(avgCard);
        if (compare < 0) {
          // Take the lacking range from the next partition.
          long require = avgCard.subtract(BigDecimal.valueOf(current.getCount())).round(MathContext.DECIMAL128).longValue();
          for (int j = i + 1; j < buckets.size() && require > 0; j++) {
            Bucket nextBucket = buckets.get(j);
            long takeAmount = require < nextBucket.getCount() ? require : nextBucket.getCount();
            Tuple newEnd = increment(sortSpecs, columnStatsList, current.getEndKey(), current.getBase(), takeAmount);
            current.getKey().setEnd(newEnd);
            current.incCount(takeAmount);
            nextBucket.getKey().setStart(newEnd);
            nextBucket.incCount(-1 * takeAmount);
            require -= takeAmount;
          }

        } else if (compare > 0) {
          // Pass the remaining range to the next partition.
          long passAmount = BigDecimal.valueOf(current.getCount()).subtract(avgCard).round(MathContext.DECIMAL128).longValue();
          Tuple newEnd = increment(sortSpecs, columnStatsList, current.getEndKey(), current.getBase(), -1 * passAmount);
          passed = histogram.createBucket(new TupleRange(newEnd, current.getEndKey(), current.getKey().getBase(), comparator), passAmount);
          current.getKey().setEnd(newEnd);
          current.incCount(-1 * passAmount);
        }
      }
    }
  }
}
