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
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.FreqHistogram.Bucket;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

public class HistogramUtil {

  /**
   * Normalize ranges of buckets into the range of [0, 1).
   *
   * @param totalRange
   * @return
   */
  public static FreqHistogram normalize(final FreqHistogram histogram, final TupleRange totalRange) {
    // TODO: Type must be able to contain BigDecimal
    Schema normalizedSchema = new Schema(new Column[]{new Column("normalized", Type.FLOAT8)});
    SortSpec[] normalizedSortSpecs = new SortSpec[1];
    normalizedSortSpecs[0] = new SortSpec(normalizedSchema.getColumn(0), true, false);
    FreqHistogram normalized = new FreqHistogram(normalizedSchema, normalizedSortSpecs);

    BigDecimal totalDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(histogram.sortSpecs,
        totalRange, true));
    BigDecimal baseDiff = BigDecimal.ONE;

    MathContext mathContext = new MathContext(20, RoundingMode.HALF_DOWN);
    Tuple normalizedBase = new VTuple(normalizedSchema.size());
    BigDecimal div = baseDiff.divide(totalDiff, mathContext);
    normalizedBase.put(0, DatumFactory.createFloat8(div.doubleValue()));

    for (Bucket eachBucket: histogram.buckets.values()) {
      BigDecimal startDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(histogram.sortSpecs,
          totalRange.getStart(), eachBucket.getStartKey(), totalRange.getBase(), true).subtract(BigInteger.ONE));
      BigDecimal endDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(histogram.sortSpecs,
          totalRange.getStart(), eachBucket.getEndKey(), totalRange.getBase(), true).subtract(BigInteger.ONE));
      Tuple normalizedStartTuple = new VTuple(
          new Datum[]{
              DatumFactory.createFloat8(startDiff.divide(totalDiff, mathContext).doubleValue())
          });
      Tuple normalizedEndTuple = new VTuple(
          new Datum[]{
              DatumFactory.createFloat8(endDiff.divide(totalDiff, mathContext).doubleValue())
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
   * @param totalRange
   * @return
   */
  public static FreqHistogram denormalize(FreqHistogram normalized,
                                          Schema denormalizedSchema,
                                          SortSpec[] denormalizedSortSpecs,
                                          TupleRange totalRange) {
    FreqHistogram denormalized = new FreqHistogram(denormalizedSchema, denormalizedSortSpecs);

    // Denormalize base

    for (Bucket eachBucket: normalized.buckets.values()) {
      BigDecimal normalizedStart = BigDecimal.valueOf(eachBucket.getStartKey().getFloat8(0));
      BigDecimal normalizedEnd = BigDecimal.valueOf(eachBucket.getEndKey().getFloat8(0));





    }
    return null;
  }

  /**
   *
   * @param tuple
   * @param count
   * @param base
   * @return
   */
  public static Tuple increment(final Tuple tuple, final BigInteger count, final Tuple base) {
    // result = operand + count * base

    return null;
  }

  public static Tuple add(final SortSpec[] sortSpecs, final TupleRange minMax,
                          final Tuple t1, final Tuple t2) {
    Tuple result = new VTuple(sortSpecs.length);
    BigDecimal temp, max;
    BigDecimal[] carryAndReminder = new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ZERO};
    for (int i = sortSpecs.length; i >= 0; i--) {
      // result = carry * max + val1 + val2
      // result > max ? result - max : result; carry++

      Column column = sortSpecs[i].getSortKey();
      switch (column.getDataType().getType()) {
        case CHAR:
          max = BigDecimal.valueOf(Character.MAX_VALUE);
          temp = carryAndReminder[0].multiply(max).
              add(BigDecimal.valueOf(t1.getChar(i) + t2.getChar(i)));
          carryAndReminder = calculateCarryAndReminder(temp, max);
          result.put(i, DatumFactory.createChar((char) carryAndReminder[1].longValue()));
          break;
        case INT2:
          max = BigDecimal.valueOf(Short.MAX_VALUE);
          temp = carryAndReminder[0].multiply(max).
              add(BigDecimal.valueOf(t1.getInt2(i) + t2.getInt2(i)));
          carryAndReminder = calculateCarryAndReminder(temp, max);
          result.put(i, DatumFactory.createInt2(carryAndReminder[1].shortValue()));
          break;
        case INT4:
          max = BigDecimal.valueOf(Integer.MAX_VALUE);
          temp = carryAndReminder[0].multiply(max).
              add(BigDecimal.valueOf(t1.getInt4(i) + t2.getInt4(i)));
          carryAndReminder = calculateCarryAndReminder(temp, max);
          result.put(i, DatumFactory.createInt4(carryAndReminder[1].intValue()));
          break;
        case INT8:
          max = BigDecimal.valueOf(Long.MAX_VALUE);
          temp = carryAndReminder[0].multiply(max).
              add(BigDecimal.valueOf(t1.getInt8(i) + t2.getInt8(i)));
          carryAndReminder = calculateCarryAndReminder(temp, max);
          result.put(i, DatumFactory.createInt8(carryAndReminder[1].longValue()));
          break;
        case FLOAT4:
          max = BigDecimal.valueOf(Float.MAX_VALUE);
          temp = carryAndReminder[0].multiply(max).
              add(BigDecimal.valueOf(t1.getFloat4(i) + t2.getFloat4(i)));
          carryAndReminder = calculateCarryAndReminder(temp, max);
          result.put(i, DatumFactory.createFloat4(carryAndReminder[1].floatValue()));
          break;
        case FLOAT8:
          max = BigDecimal.valueOf(Double.MAX_VALUE);
          temp = carryAndReminder[0].multiply(max).
              add(BigDecimal.valueOf(t1.getFloat8(i) + t2.getFloat8(i)));
          carryAndReminder = calculateCarryAndReminder(temp, max);
          result.put(i, DatumFactory.createFloat8(carryAndReminder[1].doubleValue()));
          break;
        case TEXT:

          break;
        case DATE:
          break;
        case TIME:
          break;
        case TIMESTAMP:
          break;
        case INET4:
          break;
        default:
          throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
      }
    }

    return null;
  }

  private static BigDecimal[] calculateCarryAndReminder(BigDecimal val, BigDecimal max) {
    BigDecimal[] carryAndReminder = new BigDecimal[2];
    if (val.compareTo(max) > 0) {
      BigDecimal[] quotAndRem = val.divideAndRemainder(max);
      carryAndReminder[0] = quotAndRem[0]; // set carry as quotient
      carryAndReminder[1] = quotAndRem[1];
    } else {
      carryAndReminder[0] = BigDecimal.ZERO;
      carryAndReminder[1] = val;
    }
    return carryAndReminder;
  }
}
