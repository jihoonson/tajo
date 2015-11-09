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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class HistogramUtil {

  private static final Log LOG = LogFactory.getLog(HistogramUtil.class);

  public static AnalyzedSortSpec[] toAnalyzedSortSpecs(SortSpec[] sortSpecs, List<ColumnStats> columnStatses) {
    AnalyzedSortSpec[] result = new AnalyzedSortSpec[sortSpecs.length];
    for (int i = 0; i < sortSpecs.length; i++) {
      result[i] = new AnalyzedSortSpec(sortSpecs[i], columnStatses.get(i));
      result[i].setPureAscii(true);
    }
    return result;
  }

  public static AnalyzedSortSpec[] analyzeHistogram(FreqHistogram histogram, List<ColumnStats> columnStatses) {
    SortSpec[] sortSpecs = histogram.getSortSpecs();
    AnalyzedSortSpec[] analyzedSpecs = toAnalyzedSortSpecs(sortSpecs, columnStatses);
    for (Bucket bucket : histogram.getAllBuckets()) {
      Tuple tuple = bucket.getStartKey();
      for (int i = 0; i < sortSpecs.length; i++) {
        if (sortSpecs[i].getSortKey().getDataType().getType().equals(Type.TEXT)) {
          boolean isCurrentPureAscii = StringUtils.isPureAscii(tuple.getText(i));
          if (analyzedSpecs[i].isPureAscii()) {
            analyzedSpecs[i].setPureAscii(isCurrentPureAscii);
          }
          if (isCurrentPureAscii) {
            analyzedSpecs[i].setMaxLength(Math.max(analyzedSpecs[i].getMaxLength(), tuple.getText(i).length()));
          } else {
            analyzedSpecs[i].setMaxLength(Math.max(analyzedSpecs[i].getMaxLength(), tuple.getUnicodeChars(i).length));
          }
        }
      }
    }
    return analyzedSpecs;
  }

  public static Schema sortSpecsToSchema(SortSpec[] sortSpecs) {
    Schema schema = new Schema();
    for (SortSpec spec : sortSpecs) {
      schema.addColumn(spec.getSortKey());
    }

    return schema;
  }

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

  public static List<Bucket> splitBucket(FreqHistogram histogram, AnalyzedSortSpec[] sortSpecs,
                                         Bucket bucket, Tuple interval) {
    Comparator<Tuple> comparator = histogram.getComparator();
    List<Bucket> splits = new ArrayList<>();

    if (bucket.getCount() == 1 ||
        bucket.getBase().equals(TupleRangeUtil.createMinBaseTuple(histogram.getSortSpecs()))) {
      splits.add(bucket);
    } else {
      long remaining = bucket.getCount();
      Tuple start = bucket.getStartKey();
      Tuple end = HistogramUtil.increment(sortSpecs, start, interval, 1);
      BigDecimal normalizedStart = normalize(sortSpecs, start);
      BigDecimal totalDiff = normalize(sortSpecs, bucket.getEndKey()).subtract(normalizedStart);
      BigDecimal totalCount = BigDecimal.valueOf(bucket.getCount());

      while (comparator.compare(end, bucket.getEndKey()) < 0) {
        // count = totalCount * (end - start) / totalDiff
        long count = totalCount.multiply(normalize(sortSpecs, end).subtract(normalizedStart))
            .divide(totalDiff, MathContext.DECIMAL128).longValue();
        splits.add(histogram.createBucket(new TupleRange(start, end, interval, comparator), count));
        start = end;
        try {
          end = HistogramUtil.increment(sortSpecs, start, interval, 1);
        } catch (TajoInternalError e) {
          // TODO
          break;
        }
        normalizedStart = normalize(sortSpecs, start);
        remaining -= count;
      }

      if (!end.equals(bucket.getEndKey())) {
        // TODO: interval is invalid
        splits.add(histogram.createBucket(new TupleRange(start, bucket.getEndKey(), interval, comparator), remaining));
      }
    }

    return splits;
  }

  public static Tuple getPositiveInfiniteTuple(int size) {
    Tuple tuple = new VTuple(size);
    for (int i = 0; i < size; i++) {
      tuple.put(i, PositiveInfiniteDatum.get());
    }
    return tuple;
  }

  public static Datum getLastValue(SortSpec[] sortSpecs, List<ColumnStats> columnStatsList, int i) {
    return columnStatsList.get(i).hasNullValue() ?
        sortSpecs[i].isNullFirst() ? columnStatsList.get(i).getMaxValue() : NullDatum.get()
        : columnStatsList.get(i).getMaxValue();
  }

  public static Datum getFirstValue(SortSpec[] sortSpecs, List<ColumnStats> columnStatsList, int i) {
    return columnStatsList.get(i).hasNullValue() ?
        sortSpecs[i].isNullFirst() ? NullDatum.get() : columnStatsList.get(i).getMinValue()
        : columnStatsList.get(i).getMinValue();
  }

  public static BigDecimal[] getMinMaxIncludeNull(final AnalyzedSortSpec sortSpec) {
    BigDecimal min, max;
    // TODO: min value may not be zero
    switch (sortSpec.getType()) {
      case BOOLEAN:
        max = BigDecimal.ONE;
        min = BigDecimal.ZERO;
        break;
      case INT2:
        max = BigDecimal.valueOf(sortSpec.getMaxValue().asInt2());
        min = BigDecimal.valueOf(sortSpec.getMinValue().asInt2());
        break;
      case INT4:
      case DATE:
      case INET4:
        max = BigDecimal.valueOf(sortSpec.getMaxValue().asInt4());
        min = BigDecimal.valueOf(sortSpec.getMinValue().asInt4());
        break;
      case INT8:
      case TIME:
        max = BigDecimal.valueOf(sortSpec.getMaxValue().asInt8());
        min = BigDecimal.valueOf(sortSpec.getMinValue().asInt8());
        break;
      case FLOAT4:
        max = BigDecimal.valueOf(sortSpec.getMaxValue().asFloat4());
        min = BigDecimal.valueOf(sortSpec.getMinValue().asFloat4());
        break;
      case FLOAT8:
        max = BigDecimal.valueOf(sortSpec.getMaxValue().asFloat8());
        min = BigDecimal.valueOf(sortSpec.getMinValue().asFloat8());
        break;
      case CHAR:
        max = BigDecimal.valueOf(sortSpec.getMaxValue().asChar());
        min = BigDecimal.valueOf(sortSpec.getMinValue().asChar());
        break;
      case TEXT:
        if (sortSpec.isPureAscii()) {
          max = new BigDecimal(new BigInteger(sortSpec.getMaxValue().asByteArray()));
          min = new BigDecimal(new BigInteger(sortSpec.getMinValue().asByteArray()));
        } else {
          max = unicodeCharsToBigDecimal(sortSpec.getMaxValue().asUnicodeChars());
          min = unicodeCharsToBigDecimal(sortSpec.getMinValue().asUnicodeChars());
        }
        break;
      case TIMESTAMP:
        max = BigDecimal.valueOf(((TimestampDatum) sortSpec.getMaxValue()).getJavaTimestamp());
        min = BigDecimal.valueOf(((TimestampDatum) sortSpec.getMinValue()).getJavaTimestamp());
        break;
      default:
        throw new UnsupportedOperationException(sortSpec.getType() + " is not supported yet");
    }
    if (sortSpec.hasNullValue()) {
      if (sortSpec.isNullFirst()) {
        min = min.subtract(BigDecimal.ONE);
      } else {
        max = max.add(BigDecimal.ONE);
      }
    }
    return new BigDecimal[] {min, max};
  }

  private static BigDecimal denormalizeVal(final AnalyzedSortSpec sortSpec,
                                        final BigDecimal val) {
    return val.multiply(sortSpec.getTransformedMax()).add(sortSpec.getMin());
  }

  private static Datum denormalizeToDatum(final AnalyzedSortSpec sortSpec,
                                          final BigDecimal val) {
    if (sortSpec.isNullFirst() && val.equals(BigDecimal.ZERO)) {
      return NullDatum.get();
    } else if (!sortSpec.isNullFirst() && val.equals(BigDecimal.ONE)) {
      return NullDatum.get();
    }

    BigDecimal denormalized = denormalizeVal(sortSpec, val);

    switch (sortSpec.getType()) {
      case BOOLEAN:
        return denormalized.longValue() % 2 == 0 ? BooleanDatum.FALSE : BooleanDatum.TRUE;
      case CHAR:
        return DatumFactory.createChar((char) denormalized.longValue());
      case INT2:
        return DatumFactory.createInt2(denormalized.shortValue());
      case INT4:
        return DatumFactory.createInt4(denormalized.intValue());
      case INT8:
        return DatumFactory.createInt8(denormalized.longValue());
      case FLOAT4:
        return DatumFactory.createFloat4(denormalized.floatValue());
      case FLOAT8:
        return DatumFactory.createFloat8(denormalized.doubleValue());
      case TEXT:
        if (sortSpec.isPureAscii()) {
          return DatumFactory.createText(denormalized.toBigInteger().toByteArray());
        } else {
          return DatumFactory.createText(Convert.chars2utf(bigDecimalToUnicodeChars(denormalized)));
        }
      case DATE:
        return DatumFactory.createDate(denormalized.intValue());
      case TIME:
        return DatumFactory.createTime(denormalized.longValue());
      case TIMESTAMP:
        return DatumFactory.createTimestampDatumWithJavaMillis(denormalized.longValue());
      case INET4:
        return DatumFactory.createInet4((int) denormalized.longValue());
      default:
        throw new UnsupportedOperationException(sortSpec.getType() + " is not supported yet");
    }
  }

  public static Tuple denormalize(final AnalyzedSortSpec[] analyzedSpecs,
                                  final BigDecimal normTuple,
                                  final BigDecimal normInterval) {
    Tuple result = new VTuple(analyzedSpecs.length);
    BigDecimal remainder = normTuple.divide(BigDecimal.TEN.pow(analyzedSpecs.length - 1), MathContext.DECIMAL128);
    for (int i = 0; i < analyzedSpecs.length; i++) {
      if (i > 0) {
        remainder = remainder.multiply(BigDecimal.TEN);
      }

      BigDecimal[] quotiAndRem = remainder.divideAndRemainder(normInterval, MathContext.DECIMAL128);
      BigDecimal normDatum = quotiAndRem[0].multiply(normInterval);
      result.put(i, denormalizeToDatum(analyzedSpecs[i], normDatum));
      remainder = quotiAndRem[1];
    }
    return result;
  }

  /**
   * Normalize the given value into a real value in the range of [0, 1].
   *
   * @param sortSpec analyzed sort spec
   * @param val denormalized value
   * @return normalized value
   */
  private static BigDecimal normalize(final AnalyzedSortSpec sortSpec, final BigDecimal val) {
    return val.subtract(sortSpec.getMin())
        .divide(sortSpec.getTransformedMax(), MathContext.DECIMAL128);
  }

  /**
   * Normalize the given datum into a real value in the range of [0, 1].
   *
   * @param sortSpec analyzed sort spec
   * @param val non-text datum
   * @return normalized value
   */
  private static BigDecimal normalize(final AnalyzedSortSpec sortSpec,
                                      final Datum val) {
    if (val.isNull()) {
      return sortSpec.isNullFirst() ? BigDecimal.ZERO : BigDecimal.ONE;
    } else {
      BigDecimal normNumber;
      switch (sortSpec.getType()) {
        case BOOLEAN:
          normNumber = val.isTrue() ? BigDecimal.ONE : BigDecimal.ZERO;
          break;
        case CHAR:
          normNumber = BigDecimal.valueOf(val.asChar());
          break;
        case INT2:
          normNumber = BigDecimal.valueOf(val.asInt2());
          break;
        case INT4:
          normNumber = BigDecimal.valueOf(val.asInt4());
          break;
        case INT8:
          normNumber = BigDecimal.valueOf(val.asInt8());
          break;
        case FLOAT4:
          normNumber = BigDecimal.valueOf(val.asFloat4());
          break;
        case FLOAT8:
          normNumber = BigDecimal.valueOf(val.asFloat8());
          break;
        case DATE:
          normNumber = BigDecimal.valueOf(val.asInt4());
          break;
        case TIME:
          normNumber = BigDecimal.valueOf(val.asInt8());
          break;
        case TIMESTAMP:
          normNumber = BigDecimal.valueOf(((TimestampDatum) val).getJavaTimestamp());
          break;
        case INET4:
          normNumber = BigDecimal.valueOf(val.asInt8());
          break;
        default:
          throw new UnsupportedOperationException(sortSpec.getType() + " is not supported yet");
      }
      return normalize(sortSpec, normNumber);
    }
  }

  private static BigDecimal bytesToBigDecimal(final byte[] bytes,
                                              final int length) {
    if (bytes.length == 0) {
      return BigDecimal.ZERO;
    } else {
      byte[] padded = Bytes.padTail(bytes, bytes.length - length);
      return new BigDecimal(new BigInteger(padded));
    }
  }

  private static BigDecimal unicodeCharsToBigDecimal(final char[] unicodeChars,
                                                     final int length) {
    if (unicodeChars.length == 0) {
      return BigDecimal.ZERO;
    } else {
      char[] padded = StringUtils.padTail(unicodeChars, length);
      return unicodeCharsToBigDecimal(padded);
    }
  }

  /**
   * Normalize the given text datum into a real value in the range of [0, 1].
   *
   * @param sortSpec analyzed sort spec
   * @param val text datum
   * @return normalized value
   */
  private static BigDecimal normalizeText(final AnalyzedSortSpec sortSpec,
                                          final Datum val) {
    if (val.isNull()) {
      return sortSpec.isNullFirst() ? BigDecimal.ZERO : BigDecimal.ONE;
    } else if (val.size() == 0) {
      return BigDecimal.ZERO;
    } else {
      if (sortSpec.isPureAscii()) {
        return normalize(sortSpec, bytesToBigDecimal(val.asByteArray(), sortSpec.getMaxLength()));
      } else {
        return normalize(sortSpec, unicodeCharsToBigDecimal(val.asUnicodeChars(), sortSpec.getMaxLength()));
      }
    }
  }

  /**
   * Normalize the given tuple into a real value.
   *
   * @param analyzedSpecs
   * @param tuple
   * @return
   */
  public static BigDecimal normalize(AnalyzedSortSpec[] analyzedSpecs, Tuple tuple) {
    BigDecimal result = BigDecimal.ZERO;
    BigDecimal exponent = BigDecimal.ONE;
    for (int i = analyzedSpecs.length - 1; i >= 0; i--) {
      BigDecimal normalized;
      if (analyzedSpecs[i].getType().equals(Type.TEXT)) {
        normalized = normalizeText(analyzedSpecs[i], tuple.asDatum(i));
      } else {
        normalized = normalize(analyzedSpecs[i], tuple.asDatum(i));
      }
      result = result.add(normalized.multiply(exponent));
      exponent = exponent.multiply(BigDecimal.TEN);
    }
    return result;
  }

  public static Tuple diff(final Comparator<Tuple> comparator,
                           final AnalyzedSortSpec[] sortSpecs,
                           final Tuple first, final Tuple second) {
    return null;
//    // TODO: handle infinite datums
//    // TODO: handle null datums
//    Tuple result = new VTuple(sortSpecs.length);
//    BigDecimal temp;
//    BigDecimal[] minMax;
//    BigDecimal[] carryAndRemainder = new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ZERO};
//
//    Tuple large, small;
//    if (comparator.compare(first, second) < 0) {
//      large = second;
//      small = first;
//    } else {
//      large = first;
//      small = second;
//    }
//
//    for (int i = sortSpecs.length-1; i >= 0; i--) {
//      BigDecimal normalizedLarge, normalizedSmall;
//      Column column = sortSpecs[i].getSortKey();
//
//      minMax = getMinMaxIncludeNull(sortSpecs[i]);
//
//      BigDecimal min = minMax[0];
//      BigDecimal max = minMax[1];
//      BigDecimal normalizedMax = max.subtract(min);
//
//      if (large.isBlankOrNull(i)) {
//        normalizedLarge = sortSpecs[i].isNullFirst() ? min : max;
//      } else {
//        switch (column.getDataType().getType()) {
//          case BOOLEAN:
//            normalizedLarge = large.getBool(i) ? BigDecimal.ZERO : BigDecimal.ONE;
//            break;
//          case CHAR:
//          case INT2:
//          case INT4:
//          case INT8:
//          case FLOAT4:
//          case FLOAT8:
//          case DATE:
//          case TIME:
//          case TIMESTAMP:
//          case INET4:
//            normalizedLarge = normalize(sortSpecs[i], large.asDatum(i), min, max);
//            break;
//          case TEXT:
//            if (sortSpecs[i].isPureAscii()) {
//              byte[] op = large.getBytes(i);
//              normalizedLarge = bytesToBigDecimal(op, sortSpecs[i].getMaxLength());
//            } else {
//              char[] op = large.getUnicodeChars(i);
//              normalizedLarge = unicodeCharsToBigDecimal(op, sortSpecs[i].getMaxLength());
//            }
//            break;
//          default:
//            throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
//        }
//      }
//
//      if (small.isBlankOrNull(i)) {
//        normalizedSmall = sortSpecs[i].isNullFirst() ? min : max;
//      } else {
//        switch (column.getDataType().getType()) {
//          case BOOLEAN:
//            normalizedSmall = small.getBool(i) ? BigDecimal.ZERO : BigDecimal.ONE;
//            break;
//          case CHAR:
//          case INT2:
//          case INT4:
//          case INT8:
//          case FLOAT4:
//          case FLOAT8:
//          case DATE:
//          case TIME:
//          case TIMESTAMP:
//          case INET4:
//            normalizedSmall = normalize(sortSpecs[i], small.asDatum(i), min, max);
//            break;
//          case TEXT:
//            if (sortSpecs[i].isPureAscii()) {
//              byte[] op = small.getBytes(i);
//              normalizedSmall = bytesToBigDecimal(op, sortSpecs[i].getMaxLength());
//            } else {
//              char[] op = small.getUnicodeChars(i);
//              normalizedSmall = unicodeCharsToBigDecimal(op, sortSpecs[i].getMaxLength());
//            }
//            break;
//          default:
//            throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
//        }
//      }
//
//      normalizedLarge = normalizedLarge.subtract(min);
//      normalizedSmall = normalizedSmall.subtract(min);
//
//      temp = carryAndRemainder[0]
//          .add(normalizedLarge)
//          .subtract(normalizedSmall);
//      carryAndRemainder = calculateCarryAndRemainder(temp, normalizedMax);
//      result.put(i, denormalize(sortSpecs[i], carryAndRemainder[1], BigDecimal.ZERO, normalizedMax));
//    }
//
//    if (!carryAndRemainder[0].equals(BigDecimal.ZERO)) {
//      throw new TajoInternalError("Overflow");
//    }
//
//    return result;
  }

  /**
   * Increment the tuple with the amount of the product of <code>count</code> and <code>baseTuple</code>.
   * If the <code>count</code> is a negative value, it will work as decrement.
   *
   * @param sortSpecs an array of sort specifications
   * @param operand tuple to be incremented
   * @param baseTuple base tuple
   * @param count increment count. If this value is negative, this method works as decrement.
   * @return incremented tuple
   */
  public static Tuple increment(AnalyzedSortSpec[] sortSpecs, final Tuple operand, final Tuple baseTuple, long count) {
//    // TODO: handle infinite datums
//    // TODO: handle null datums
//    Tuple result = new VTuple(sortSpecs.length);
//    BigDecimal add, temp;
//    BigDecimal[] minMax;
//    BigDecimal[] carryAndRemainder = new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ZERO};
//
//    for (int i = sortSpecs.length-1; i >= 0; i--) {
//      Column column = sortSpecs[i].getSortKey();
//
//      if (operand.isBlankOrNull(i)) {
//        // If the value is null,
//        minMax = getMinMaxIncludeNull(sortSpecs[i]);
//        result.put(i, denormalize(sortSpecs[i], minMax[0], minMax[0], minMax[1]));
//
//      } else {
//
//        minMax = getMinMaxIncludeNull(sortSpecs[i]);
//
//        // result = carry * max + val1 + val2
//        // result > max ? result = remainder; update carry;
////        count *= sortSpecs[i].isAscending() ? 1 : -1;
//        BigDecimal min = minMax[0];
//        BigDecimal max = minMax[1];
//        BigDecimal normalizedMax = max.subtract(min);
//        BigDecimal normalizedOp, normalizedBase;
//
//        switch (column.getDataType().getType()) {
//          case BOOLEAN:
//            // add = base * count
//            add = BigDecimal.valueOf(count);
//            // result = carry + val + add
//            temp = operand.getBool(i) ? BigDecimal.ONE : BigDecimal.ZERO;
//            temp = carryAndRemainder[0].add(temp).add(add);
//            carryAndRemainder = calculateCarryAndRemainder(temp, normalizedMax);
//            break;
//          case CHAR:
//          case INT2:
//          case INT4:
//          case INT8:
//          case FLOAT4:
//          case FLOAT8:
//          case DATE:
//          case TIME:
//          case TIMESTAMP:
//          case INET4:
//            normalizedOp = normalize(sortSpecs[i], operand.asDatum(i), min, max)
//                .subtract(min);
//            normalizedBase = normalize(sortSpecs[i], baseTuple.asDatum(i), min, max);
////                .subtract(min);
//            // add = (base - min) * count
//            add = normalizedBase
//                .multiply(BigDecimal.valueOf(count));
//            // result = carry + (val - min) + add
//            temp = carryAndRemainder[0]
//                .add(normalizedOp)
//                .add(add);
//            carryAndRemainder = calculateCarryAndRemainder(temp, normalizedMax);
//            break;
//          case TEXT:
//            if (sortSpecs[i].isPureAscii()) {
//              if (operand.isBlankOrNull(i)) {
//                normalizedOp = sortSpecs[i].isNullFirst() ? min : max;
//              } else {
//                byte[] op = operand.getBytes(i);
//                normalizedOp = bytesToBigDecimal(op, sortSpecs[i].getMaxLength());
//              }
//              if (baseTuple.isBlankOrNull(i)) {
//                normalizedBase = sortSpecs[i].isNullFirst() ? min : max;
//              } else {
//                byte[] base = baseTuple.getBytes(i);
//                normalizedBase = bytesToBigDecimal(base, sortSpecs[i].getMaxLength());
//              }
//            } else {
//              if (operand.isBlankOrNull(i)) {
//                normalizedOp = sortSpecs[i].isNullFirst() ? min : max;
//              } else {
//                char[] op = operand.getUnicodeChars(i);
//                normalizedOp = unicodeCharsToBigDecimal(op, sortSpecs[i].getMaxLength());
//              }
//              if (baseTuple.isBlankOrNull(i)) {
//                normalizedBase = sortSpecs[i].isNullFirst() ? min : max;
//              } else {
//                char[] base = baseTuple.getUnicodeChars(i);
//                normalizedBase = unicodeCharsToBigDecimal(base, sortSpecs[i].getMaxLength());
//              }
//            }
//            normalizedOp = normalizedOp.subtract(min);
////            normalizedBase = normalizedBase.subtract(min);
//
//            add = normalizedBase
//                .multiply(BigDecimal.valueOf(count));
//            temp = carryAndRemainder[0]
//                .add(normalizedOp)
//                .add(add);
//            carryAndRemainder = calculateCarryAndRemainder(temp, normalizedMax);
//            break;
//          default:
//            throw new UnsupportedOperationException(column.getDataType() + " is not supported yet");
//        }
//
//        carryAndRemainder[1] = carryAndRemainder[1].add(min);
//        if (carryAndRemainder[1].compareTo(max) >= 0) {
//          carryAndRemainder[0] = carryAndRemainder[0].add(BigDecimal.ONE);
//          carryAndRemainder[1] = carryAndRemainder[1].subtract(max);
//        }
//        if (carryAndRemainder[1].compareTo(max) > 0 ||
//            carryAndRemainder[1].compareTo(min) < 0) {
//          throw new TajoInternalError("Invalid remainder");
//        }
//
////        if (columnStatsList.get(i).hasNullValue() && carryAndRemainder[0].compareTo(BigDecimal.ZERO) > 0) {
////          // null last ? carry--;
////          // remainder == 0 ? put null
////          if (!sortSpecs[i].isNullFirst()) {
////            carryAndRemainder[0] = carryAndRemainder[0].subtract(BigDecimal.ONE);
////          }
////          if (carryAndRemainder[1].equals(BigDecimal.ZERO)) {
////            result.put(i, NullDatum.get());
////          }
////        }
//
//        if (result.isBlank(i)) {
//          result.put(i, denormalize(sortSpecs[i], carryAndRemainder[1], min, max));
//        }
//      }
//    }
//
//    if (!carryAndRemainder[0].equals(BigDecimal.ZERO)) {
//      throw new TajoInternalError("Overflow");
//    }
//
//    return result;
    return null;
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
   * @param normalizedVal
   * @param normalizedMax
   * @return
   */
  private static BigDecimal[] calculateCarryAndRemainder(BigDecimal normalizedVal, BigDecimal normalizedMax) {
    // TODO: if val is negative??
    BigDecimal[] carryAndRemainder = new BigDecimal[2];
    if (normalizedVal.compareTo(normalizedMax) > 0) {
      // quote = val / max
      // remainder = val % max
      BigDecimal[] quotAndRem = normalizedVal.divideAndRemainder(normalizedMax, MathContext.DECIMAL128);
      carryAndRemainder[0] = quotAndRem[0]; // set carry as quotient
      carryAndRemainder[1] = quotAndRem[1];
    } else if (normalizedVal.compareTo(BigDecimal.ZERO) < 0) {
      // quote = -1 * ceil( -val / max )
      // remainder = max * -quote + val
      carryAndRemainder[0] = normalizedVal.abs().divide(normalizedMax, BigDecimal.ROUND_CEILING);
      carryAndRemainder[1] = normalizedMax.multiply(carryAndRemainder[0]).add(normalizedVal);
      carryAndRemainder[0] = carryAndRemainder[0].multiply(BigDecimal.valueOf(-1));
    } else {
      carryAndRemainder[0] = BigDecimal.ZERO;
      carryAndRemainder[1] = normalizedVal;
    }
    return carryAndRemainder;
  }

//  public static void normalize(FreqHistogram histogram, List<ColumnStats> columnStatsList) {
//    List<Bucket> buckets = histogram.getSortedBuckets();
//    Tuple[] candidates = new Tuple[] {
//        buckets.get(0).getEndKey(),
//        buckets.get(buckets.size() - 1).getEndKey()
//    };
//    for (int i = 0; i < columnStatsList.size(); i++) {
//      ColumnStats columnStats = columnStatsList.get(i);
//      for (Tuple key : candidates) {
//        if (key.isInfinite(i) || key.isBlankOrNull(i)) {
//          continue;
//        }
//        switch (columnStats.getColumn().getDataType().getType()) {
//          case BOOLEAN:
//            // ignore
//            break;
//          case INT2:
//            if (key.getInt2(i) > columnStats.getMaxValue().asInt2()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getInt2(i) < columnStats.getMinValue().asInt2()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case INT4:
//            if (key.getInt4(i) > columnStats.getMaxValue().asInt4()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getInt4(i) < columnStats.getMinValue().asInt4()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case INT8:
//            if (key.getInt8(i) > columnStats.getMaxValue().asInt8()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getInt8(i) < columnStats.getMinValue().asInt8()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case FLOAT4:
//            if (key.getFloat4(i) > columnStats.getMaxValue().asFloat4()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getFloat4(i) < columnStats.getMinValue().asFloat4()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case FLOAT8:
//            if (key.getFloat8(i) > columnStats.getMaxValue().asFloat8()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getFloat8(i) < columnStats.getMinValue().asFloat8()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case CHAR:
//            if (key.getChar(i) > columnStats.getMaxValue().asChar()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getChar(i) < columnStats.getMinValue().asChar()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case TEXT:
//            if (key.getText(i).compareTo(columnStats.getMaxValue().asChars()) > 0) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getText(i).compareTo(columnStats.getMinValue().asChars()) < 0) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case DATE:
//            if (key.getInt4(i) > columnStats.getMaxValue().asInt4()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getInt4(i) < columnStats.getMinValue().asInt4()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case TIME:
//            if (key.getInt8(i) > columnStats.getMaxValue().asInt8()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getInt8(i) < columnStats.getMinValue().asInt8()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case TIMESTAMP:
//            if (key.getInt8(i) > columnStats.getMaxValue().asInt8()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getInt8(i) < columnStats.getMinValue().asInt8()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          case INET4:
//            if (key.getInt4(i) > columnStats.getMaxValue().asInt4()) {
//              key.put(i, PositiveInfiniteDatum.get());
//            } else if (key.getInt4(i) < columnStats.getMinValue().asInt4()) {
//              key.put(i, NegativeInfiniteDatum.get());
//            }
//            break;
//          default:
//            throw new UnsupportedOperationException(columnStats.getColumn().getDataType().getType()
//                + " is not supported yet");
//        }
//      }
//    }
//  }

  /**
   * Makes the start and end keys of the TEXT type equal in length.
   *
   * @param histogram
   */
  public static void normalizeLength(FreqHistogram histogram) {
    SortSpec[] sortSpecs = histogram.getSortSpecs();
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
                                       final AnalyzedSortSpec[] sortSpecs) {
//    List<Bucket> buckets = normalizedHistogram.getSortedBuckets();
    List<Bucket> buckets = histogram.getSortedBuckets();
    Comparator<Tuple> comparator = histogram.getComparator();
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
//      if (!hasInfiniteDatum(current.getEndKey()) &&
//          !increment(sortSpecs, columnStatsList, current.getStartKey(), current.getBase(), 1, isPureAscii, maxLength).equals(current.getEndKey())) {
      if (current.getCount() > 1) {
        LOG.info("start refine from the last");
        int compare = BigDecimal.valueOf(current.getCount()).compareTo(avgCard);
        if (compare < 0) {
          // Take the lacking range from the next partition.
          long require = avgCard.subtract(BigDecimal.valueOf(current.getCount())).round(MathContext.DECIMAL128).longValue();
          for (int j = i - 1; j >= 0 && require > 0; j--) {
            Bucket nextBucket = buckets.get(j);
            long takeAmount = require < nextBucket.getCount() ? require : nextBucket.getCount();
            Tuple newStart = increment(sortSpecs, current.getStartKey(), nextBucket.getBase(), -1 * takeAmount);
            current.getKey().setStart(newStart);
            current.incCount(takeAmount);
            nextBucket.getKey().setEnd(newStart);
            nextBucket.incCount(-1 * takeAmount);
            require -= takeAmount;
          }

        } else if (compare > 0) {
          // Pass the remaining range to the next partition.
          long passAmount = BigDecimal.valueOf(current.getCount()).subtract(avgCard).round(MathContext.DECIMAL128).longValue();
          Tuple newStart = increment(sortSpecs, current.getStartKey(), current.getBase(), passAmount);
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
//      if (!hasInfiniteDatum(current.getEndKey()) &&
//          !increment(sortSpecs, columnStatsList, current.getStartKey(), current.getBase(), 1, isPureAscii, maxLength).equals(current.getEndKey())) {
      if (current.getCount() > 1) {
        LOG.info("start refine from the first");
        int compare = BigDecimal.valueOf(current.getCount()).compareTo(avgCard);
        if (compare < 0) {
          // Take the lacking range from the next partition.
          long require = avgCard.subtract(BigDecimal.valueOf(current.getCount())).round(MathContext.DECIMAL128).longValue();
          for (int j = i + 1; j < buckets.size() && require > 0; j++) {
            Bucket nextBucket = buckets.get(j);
            long takeAmount = require < nextBucket.getCount() ? require : nextBucket.getCount();
            Tuple newEnd = increment(sortSpecs, current.getEndKey(), current.getBase(), takeAmount);
            current.getKey().setEnd(newEnd);
            current.incCount(takeAmount);
            nextBucket.getKey().setStart(newEnd);
            nextBucket.incCount(-1 * takeAmount);
            require -= takeAmount;
          }

        } else if (compare > 0) {
          // Pass the remaining range to the next partition.
          long passAmount = BigDecimal.valueOf(current.getCount()).subtract(avgCard).round(MathContext.DECIMAL128).longValue();
          Tuple newEnd = increment(sortSpecs, current.getEndKey(), current.getBase(), -1 * passAmount);
          passed = histogram.createBucket(new TupleRange(newEnd, current.getEndKey(), current.getKey().getBase(), comparator), passAmount);
          current.getKey().setEnd(newEnd);
          current.incCount(-1 * passAmount);
        }
      }
    }
  }
}
