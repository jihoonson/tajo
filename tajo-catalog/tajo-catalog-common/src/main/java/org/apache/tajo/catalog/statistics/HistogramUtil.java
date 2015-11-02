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

import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.FreqHistogram.Bucket;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.BooleanDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;

public class HistogramUtil {

//  public final static MathContext MATH_CONTEXT = new MathContext(50, RoundingMode.HALF_DOWN);

  /**
   * Normalize ranges of buckets into the range of [0, 1).
   *
   * @param histogram
   * @param totalRange
   * @return
   */
  public static FreqHistogram normalize(final FreqHistogram histogram, final TupleRange totalRange,
                                        final BigInteger totalCardinality) {
    // TODO: Type must be able to contain BigDecimal
    Schema normalizedSchema = new Schema(new Column[]{new Column("normalized", Type.FLOAT8)});
    SortSpec[] normalizedSortSpecs = new SortSpec[1];
    normalizedSortSpecs[0] = new SortSpec(normalizedSchema.getColumn(0), true, false);
    FreqHistogram normalized = new FreqHistogram(normalizedSchema, normalizedSortSpecs);

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
   *
   * @param sortSpecs
   * @param columnStatsList
   * @param operand
   * @param baseTuple
   * @param count
   * @return
   */
  public static Tuple increment(final SortSpec[] sortSpecs, final List<ColumnStats> columnStatsList,
                                final Tuple operand, final Tuple baseTuple, long count) {
    Tuple result = new VTuple(sortSpecs.length);
    BigDecimal max, min, add, temp;
    BigDecimal[] carryAndReminder = new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ZERO};
    for (int i = sortSpecs.length-1; i >= 0; i--) {
      // result = carry * max + val1 + val2
      // result > max ? result = reminder; update carry;

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
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asChar());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asChar());
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getChar(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getChar(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createChar((char) carryAndReminder[1].longValue()));
        break;
        case INT2:
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt2());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt2());
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt2(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt2(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createInt2(carryAndReminder[1].shortValue()));
          break;
        case INT4:
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt4());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt4());
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt4(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt4(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createInt4(carryAndReminder[1].intValue()));
          break;
        case INT8:
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt8());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt8());
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt8(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt8(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createInt8(carryAndReminder[1].longValue()));
          break;
        case FLOAT4:
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asFloat4());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asFloat4());
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getFloat4(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getFloat4(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createFloat4(carryAndReminder[1].floatValue()));
          break;
        case FLOAT8:
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asFloat8());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asFloat8());
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getFloat8(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getFloat8(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createFloat8(carryAndReminder[1].doubleValue()));
          break;
        case TEXT:
          boolean isPureAscii = StringUtils.isPureAscii(columnStatsList.get(i).getMinValue().asChars()) &&
              StringUtils.isPureAscii(columnStatsList.get(i).getMaxValue().asChars());
          if (isPureAscii) {
            max = new BigDecimal(new BigInteger(columnStatsList.get(i).getMaxValue().asByteArray()));
            min = new BigDecimal(new BigInteger(columnStatsList.get(i).getMinValue().asByteArray()));
            // add = base * count
            add = new BigDecimal(new BigInteger(baseTuple.getBytes(i))).multiply(BigDecimal.valueOf(count));
            // result = carry + val + add
            temp = carryAndReminder[0].add(new BigDecimal(new BigInteger(operand.getBytes(i)))).add(add);
            carryAndReminder = calculateCarryAndReminder(temp, max, min);
            result.put(i, DatumFactory.createText(carryAndReminder[1].toBigInteger().toByteArray()));
          } else {
            // TODO
            throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
          }
          break;
        case DATE:
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt4());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt4());
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt4(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt4(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createDate(carryAndReminder[1].intValue()));
          break;
        case TIME:
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt8());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt8());
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt8(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt8(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createTime(carryAndReminder[1].longValue()));
          break;
        case TIMESTAMP:
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt8());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt8());
          // add = base * count
          add = BigDecimal.valueOf(baseTuple.getInt8(i)).multiply(BigDecimal.valueOf(count));
          // result = carry + val + add
          temp = carryAndReminder[0].add(BigDecimal.valueOf(operand.getInt8(i))).add(add);
          carryAndReminder = calculateCarryAndReminder(temp, max, min);
          result.put(i, DatumFactory.createTimestmpDatumWithJavaMillis(carryAndReminder[1].longValue()));
          break;
        case INET4:
          max = BigDecimal.valueOf(columnStatsList.get(i).getMaxValue().asInt8());
          min = BigDecimal.valueOf(columnStatsList.get(i).getMinValue().asInt8());
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

    return result;
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

    for (Bucket bucket : histogram.getAllBuckets()) {
      for (int i = 0; i < sortSpecs.length; i++) {
        if (sortSpecs[i].getSortKey().getDataType().getType() == TajoDataTypes.Type.TEXT) {
          pureAscii[i] = StringUtils.isPureAscii(bucket.getStartKey().getText(i)) &&
              StringUtils.isPureAscii(bucket.getEndKey().getText(i));
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
            byte[] startBytes;
            byte[] endBytes;
            if (range.getStart().isBlankOrNull(i)) {
              startBytes = BigInteger.ZERO.toByteArray();
            } else {
              startBytes = range.getStart().getBytes(i);
            }

            if (range.getEnd().isBlankOrNull(i)) {
              endBytes = BigInteger.ZERO.toByteArray();
            } else {
              endBytes = range.getEnd().getBytes(i);
            }

            startBytes = Bytes.padTail(startBytes, maxLenOfTextCols[i] - startBytes.length);
            endBytes = Bytes.padTail(endBytes, maxLenOfTextCols[i] - endBytes.length);
            range.getStart().put(i, DatumFactory.createText(startBytes));
            range.getEnd().put(i, DatumFactory.createText(endBytes));

          } else {
            char[] startChars;
            char[] endChars;
            if (range.getStart().isBlankOrNull(i)) {
              startChars = new char[]{0};
            } else {
              startChars = range.getStart().getUnicodeChars(i);
            }

            if (range.getEnd().isBlankOrNull(i)) {
              endChars = new char[]{0};
            } else {
              endChars = range.getEnd().getUnicodeChars(i);
            }

            startChars = StringUtils.padTail(startChars, maxLenOfTextCols[i] - startChars.length);
            endChars = StringUtils.padTail(endChars, maxLenOfTextCols[i] - endChars.length);
            range.getStart().put(i, DatumFactory.createText(new String(startChars)));
            range.getEnd().put(i, DatumFactory.createText(new String(endChars)));
          }
        }
      }
    }
  }
}
