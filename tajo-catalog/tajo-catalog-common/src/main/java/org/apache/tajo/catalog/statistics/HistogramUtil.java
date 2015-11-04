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

  /**
   * Normalize ranges of buckets into the range of [0, 1).
   *
   * @param histogram
   * @param totalRange
   * @return
   */
  public static FreqHistogram normalize(final FreqHistogram histogram,
                                        final TupleRange totalRange,
                                        final BigInteger totalCardinality) {
    // TODO: Type must be able to contain BigDecimal
    Schema normalizedSchema = new Schema(new Column[]{new Column("normalized", Type.FLOAT8)});
    SortSpec[] normalizedSortSpecs = new SortSpec[1];
    normalizedSortSpecs[0] = new SortSpec(normalizedSchema.getColumn(0), true, false);
    FreqHistogram normalized = new FreqHistogram(normalizedSchema, normalizedSortSpecs);

    // TODO: calculate diff instead of cardinality
    BigDecimal totalDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(histogram.sortSpecs,
        totalRange, true)).multiply(new BigDecimal(totalCardinality));
    BigDecimal baseDiff = BigDecimal.ONE;

    Tuple normalizedBase = new VTuple(normalizedSchema.size());
    BigDecimal div = baseDiff.divide(totalDiff, MathContext.DECIMAL128);
    normalizedBase.put(0, DatumFactory.createFloat8(div.doubleValue()));

    for (Bucket eachBucket: histogram.buckets.values()) {
      BigDecimal startDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(histogram.sortSpecs,
          totalRange.getStart(), eachBucket.getStartKey(), totalRange.getBase(), true).subtract(BigInteger.ONE));
      BigDecimal endDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(histogram.sortSpecs,
          totalRange.getStart(), eachBucket.getEndKey(), totalRange.getBase(), true).subtract(BigInteger.ONE));
      Tuple normalizedStartTuple = new VTuple(
          new Datum[]{
              DatumFactory.createFloat8(startDiff.divide(totalDiff, MathContext.DECIMAL128).doubleValue())
          });
      Tuple normalizedEndTuple = new VTuple(
          new Datum[]{
              DatumFactory.createFloat8(endDiff.divide(totalDiff, MathContext.DECIMAL128).doubleValue())
          });
      normalized.updateBucket(normalizedStartTuple, normalizedEndTuple, normalizedBase, eachBucket.getCount());
    }

    return normalized;
  }

  /**
   *
   * @param normalized
   * @param denormalizedSchema
   * @param denormalizedSortSpecs
   * @param columnStatsList
   * @param totalRange
   * @return
   */
  public static FreqHistogram denormalize(FreqHistogram normalized,
                                          Schema denormalizedSchema,
                                          SortSpec[] denormalizedSortSpecs,
                                          List<ColumnStats> columnStatsList,
                                          TupleRange totalRange) {
    FreqHistogram denormalized = new FreqHistogram(denormalizedSchema, denormalizedSortSpecs);
    Tuple start = new VTuple(totalRange.getStart());
    List<Bucket> buckets = normalized.getSortedBuckets();

    for (Bucket eachBucket: buckets) {
      Tuple end = increment(denormalizedSortSpecs, columnStatsList, start, totalRange.getBase(), eachBucket.getCount());
      denormalized.updateBucket(start, end, totalRange.getBase(), eachBucket.getCount());
      start = end;
    }
    return denormalized;
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
  public static Tuple increment(final SortSpec[] sortSpecs, @Nullable  final List<ColumnStats> columnStatsList,
                                final Tuple operand, final Tuple baseTuple, long count) {
    Tuple result = new VTuple(sortSpecs.length);
    BigDecimal max, min, add, temp;
    BigDecimal[] carryAndReminder = new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ZERO};
    for (int i = sortSpecs.length-1; i >= 0; i--) {
      if (operand.isBlankOrNull(i)) {
        result.put(i, NullDatum.get());
        continue;
      }

      // result = carry * max + val1 + val2
      // result > max ? result = reminder; update carry;

      count *= sortSpecs[i].isAscending() ? 1 : -1;
      Column column = sortSpecs[i].getSortKey();
      switch (column.getDataType().getType()) {
        case BOOLEAN:
          max = BigDecimal.ONE;
          min = BigDecimal.ZERO;
          // add = base * count
          add = BigDecimal.valueOf(count);
          // result = carry + val + add
          temp = operand.getBool(i) ? max : min;
          temp = carryAndReminder[0].add(temp).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, carryAndReminder[1].longValue() % 2 == 0 ? BooleanDatum.FALSE : BooleanDatum.TRUE);
          break;
        case CHAR:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asChar());
            min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asChar());
          } else {
            max = BigDecimal.valueOf(Character.MAX_VALUE);
            min = BigDecimal.ZERO;
          }
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getChar(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getChar(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createChar((char) carryAndReminder[1].longValue()));
        break;
        case INT2:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt2());
            min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt2());
          } else {
            max = BigDecimal.valueOf(Short.MAX_VALUE);
            min = BigDecimal.ZERO;
          }
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt2(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt2(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createInt2(carryAndReminder[1].shortValue()));
          break;
        case INT4:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt4());
            min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt4());
          } else {
            max = BigDecimal.valueOf(Integer.MAX_VALUE);
            min = BigDecimal.ZERO;
          }
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt4(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt4(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createInt4(carryAndReminder[1].intValue()));
          break;
        case INT8:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt8());
            min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt8());
          } else {
            max = BigDecimal.valueOf(Long.MAX_VALUE);
            min = BigDecimal.ZERO;
          }
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt8(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt8(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createInt8(carryAndReminder[1].longValue()));
          break;
        case FLOAT4:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asFloat4());
            min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asFloat4());
          } else {
            max = BigDecimal.valueOf(Float.MAX_VALUE);
            min = BigDecimal.ZERO;
          }
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getFloat4(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getFloat4(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createFloat4(carryAndReminder[1].floatValue()));
          break;
        case FLOAT8:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asFloat8());
            min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asFloat8());
          } else {
            max = BigDecimal.valueOf(Double.MAX_VALUE);
            min = BigDecimal.ZERO;
          }
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getFloat8(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getFloat8(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createFloat8(carryAndReminder[1].doubleValue()));
          break;
        case TEXT:
          boolean isPureAscii = StringUtils.isPureAscii(baseTuple.getText(i)) &&
              StringUtils.isPureAscii(operand.getText(i));
          if (isPureAscii) {
            if (columnStatsList != null) {
              max = new BigDecimal(new BigInteger(columnStatsList.get(i).getMaxValue().asByteArray()));
              min = new BigDecimal(new BigInteger(columnStatsList.get(i).getMinValue().asByteArray()));
            } else {
              byte[] maxBytes = new byte[baseTuple.getBytes(i).length > operand.getBytes(i).length ?
                  baseTuple.getBytes(i).length : operand.getBytes(i).length];
              byte[] minBytes = new byte[maxBytes.length];
              Arrays.fill(maxBytes, (byte) 127);
              Bytes.zero(minBytes);
              max = new BigDecimal(new BigInteger(maxBytes));
              min = new BigDecimal(new BigInteger(minBytes));
            }
            // add = base * count
            add = new BigDecimal(new BigInteger(baseTuple.getBytes(i))).multiply(BigDecimal.valueOf(count));
            // result = carry + val + add
            temp = carryAndReminder[0].add(new BigDecimal(new BigInteger(operand.getBytes(i)))).add(add);
            carryAndReminder = calculateCarryAndReminder(temp, max, min);
            result.put(i, DatumFactory.createText(carryAndReminder[1].toBigInteger().toByteArray()));
          } else {
            if (columnStatsList != null) {
              max = unicodeCharsToBigDecimal(columnStatsList.get(i).getMaxValue().asUnicodeChars());
              min = unicodeCharsToBigDecimal(columnStatsList.get(i).getMinValue().asUnicodeChars());
            } else {
              char[] maxChars = new char[baseTuple.getBytes(i).length > operand.getBytes(i).length ?
                  baseTuple.getBytes(i).length : operand.getBytes(i).length];
              char[] minChars = new char[maxChars.length];
              Arrays.fill(maxChars, Character.MAX_VALUE);
              Arrays.fill(minChars, Character.MIN_VALUE);
              max = unicodeCharsToBigDecimal(maxChars);
              min = unicodeCharsToBigDecimal(minChars);
            }

            // add = base * count
            add = unicodeCharsToBigDecimal(baseTuple.getUnicodeChars(i)).multiply(BigDecimal.valueOf(count));
            // result = carry + val + add
            temp = carryAndReminder[0].add(unicodeCharsToBigDecimal(operand.getUnicodeChars(i))).add(add);
            carryAndReminder = calculateCarryAndReminder(temp, max, min);
            result.put(i, DatumFactory.createText(Convert.chars2utf(bigDecimalToUnicodeChars(carryAndReminder[1]))));
          }
          break;
        case DATE:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt4());
            min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt4());
          } else {
            max = BigDecimal.valueOf(Integer.MAX_VALUE);
            min = BigDecimal.ZERO;
          }
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt4(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt4(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createDate(carryAndReminder[1].intValue()));
          break;
        case TIME:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt8());
            min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt8());
          } else {
            max = BigDecimal.valueOf(Long.MAX_VALUE);
            min = BigDecimal.ZERO;
          }
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt8(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt8(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createTime(carryAndReminder[1].longValue()));
          break;
        case TIMESTAMP:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(((TimestampDatum)columnStatsList.get(i).getMaxValue()).getJavaTimestamp());
            min = BigDecimal.valueOf(((TimestampDatum)columnStatsList.get(i).getMinValue()).getJavaTimestamp());
          } else {
            max = BigDecimal.valueOf(DateTimeUtil.julianTimeToJavaTime(Long.MAX_VALUE));
            min = BigDecimal.valueOf(DateTimeUtil.julianTimeToJavaTime(0));
          }
          // add = base * count
          add = BigDecimal.valueOf(((TimestampDatum)baseTuple.asDatum(i)).getJavaTimestamp()).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(((TimestampDatum)operand.asDatum(i)).getJavaTimestamp())).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createTimestampDatumWithJavaMillis(carryAndReminder[1].longValue()));
          break;
        case INET4:
          if (columnStatsList != null) {
            max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt8());
            min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt8());
          } else {
            max = BigDecimal.valueOf(Long.MAX_VALUE);
            min = BigDecimal.ZERO;
          }
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt8(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt8(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createInet4((int) carryAndReminder[1].longValue()));
          break;
        default:
          throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
      }
    }

    if (!carryAndReminder[0].equals(BigDecimal.ZERO)) {
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
      BigDecimal[] quoteAndReminder = divisor.divideAndRemainder(base, MathContext.DECIMAL128);
      divisor = quoteAndReminder[0];
      characters.add(0, (char) quoteAndReminder[1].intValue());
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
  private static BigDecimal[] calculateCarryAndReminder(BigDecimal val, BigDecimal max, BigDecimal min) {
    BigDecimal[] carryAndReminder = new BigDecimal[2];
    if (val.compareTo(max) > 0) {
      // quote = (max - min) / (val - min)
      // reminder = ((max - min) % (val - min)) + min
      BigDecimal[] quotAndRem = max.subtract(min).divideAndRemainder(val.subtract(min), MathContext.DECIMAL128);
      carryAndReminder[0] = quotAndRem[0]; // set carry as quotient
      carryAndReminder[1] = quotAndRem[1].add(min);
    } else {
      carryAndReminder[0] = BigDecimal.ZERO;
      carryAndReminder[1] = val;
    }
    return carryAndReminder;
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

//          Tuple newEnd = new VTuple(new Datum[] {
//              DatumFactory.createFloat8(
//                  // nextBucket.endKey * (takeAmount / nextBucket.count)
//                  new BigDecimal(nextBucket.getEndKey().getFloat8(0))
//                      .multiply(new BigDecimal(takeAmount))
//                      .divide(new BigDecimal(nextBucket.getCount()), MathContext.DECIMAL128).doubleValue())
//          });
//          current.getKey().setEnd(newEnd);
//          current.incCount(takeAmount);
//          nextBucket.getKey().setStart(newEnd);
//          nextBucket.incCount(-1 * takeAmount);

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

//        Tuple newEnd = new VTuple(new Datum[] {
//            DatumFactory.createFloat8(
//                // current.endKey * (passAmount / current.count)
//                new BigDecimal(current.getEndKey().getFloat8(0))
//                    .multiply(new BigDecimal(passAmount))
//                    .divide(new BigDecimal(current.getCount()), MathContext.DECIMAL128).doubleValue())
//        });
//        passed = histogram.createBucket(new TupleRange(newEnd, current.getEndKey(), current.getKey().getBase(), comparator), passAmount);
//        current.getKey().setEnd(newEnd);
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

//          Tuple newEnd = new VTuple(new Datum[] {
//              DatumFactory.createFloat8(
//                  // nextBucket.endKey * (takeAmount / nextBucket.count)
//                  new BigDecimal(nextBucket.getEndKey().getFloat8(0))
//                      .multiply(new BigDecimal(takeAmount))
//                      .divide(new BigDecimal(nextBucket.getCount()), MathContext.DECIMAL128).doubleValue())
//          });
            current.getKey().setEnd(newEnd);
            current.incCount(takeAmount);
            nextBucket.getKey().setStart(newEnd);
            nextBucket.incCount(-1 * takeAmount);
            require -= takeAmount;
          }

        } else if (compare > 0) {
          // Pass the remaining range to the next partition.
          long passAmount = BigDecimal.valueOf(current.getCount()).subtract(avgCard).round(MathContext.DECIMAL128).longValue();
//        Tuple newEnd = new VTuple(new Datum[] {
//            DatumFactory.createFloat8(
//                // current.endKey * (passAmount / current.count)
//                new BigDecimal(current.getEndKey().getFloat8(0))
//                    .multiply(new BigDecimal(passAmount))
//                    .divide(new BigDecimal(current.getCount()), MathContext.DECIMAL128).doubleValue())
//        });
          Tuple newEnd = increment(sortSpecs, columnStatsList, current.getEndKey(), current.getBase(), -1 * passAmount);
          passed = histogram.createBucket(new TupleRange(newEnd, current.getEndKey(), current.getKey().getBase(), comparator), passAmount);
          current.getKey().setEnd(newEnd);
          current.incCount(-1 * passAmount);
        }
      }
    }
  }
}
