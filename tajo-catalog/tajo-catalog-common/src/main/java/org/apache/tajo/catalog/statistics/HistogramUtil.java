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

import com.google.common.base.Preconditions;
import com.sun.tools.javac.util.Convert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TupleRange;
import org.apache.tajo.catalog.statistics.FreqHistogram.Bucket;
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
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HistogramUtil {

  private static final Log LOG = LogFactory.getLog(HistogramUtil.class);

  public final static MathContext DECIMAL128_HALF_UP = new MathContext(34, RoundingMode.HALF_UP);

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

  private static boolean isMinNormTuple(AnalyzedSortSpec[] sortSpecs, BigDecimal[] normTuple) {
    for (int i = 0; i < sortSpecs.length; i++) {
      if (!normTuple[i].equals(sortSpecs[i].getNormMin())) {
        return false;
      }
    }
    return true;
  }

  private static BigDecimal getNormMaxMax(AnalyzedSortSpec[] sortSpecs) {
    BigDecimal maxMax = BigDecimal.ZERO;
    BigDecimal minMax = null;
    for (AnalyzedSortSpec spec : sortSpecs) {
      if (maxMax.compareTo(spec.getMax()) < 0) {
        maxMax = spec.getMax();
      }
      if (minMax == null || minMax.compareTo(spec.getMax()) > 0) {
        minMax = spec.getMax();
      }
    }
//    return maxMax.divide(minMax, 34, BigDecimal.ROUND_HALF_UP);
    return maxMax;
  }

  private static int getScale(BigDecimal val) {
    int i = 0;
    while (val.compareTo(BigDecimal.ONE) < 0) {
      i++;
      val = val.multiply(BigDecimal.TEN);
    }
    return i;
  }

  public static int getMaxScale(BigDecimal[] normTuple) {
    int maxScale = 0;
    for (BigDecimal val : normTuple) {
      int scale = getScale(val);
      if (maxScale < scale) {
        maxScale = scale;
      }
    }
    return maxScale;
  }

  public static BigDecimal weightedSum(AnalyzedSortSpec[] sortSpecs, BigDecimal[] normTuple) {
//    BigDecimal maxMax = getNormMaxMax(sortSpecs);
    BigDecimal maxMax = BigDecimal.TEN.pow(getMaxScale(normTuple));
    BigDecimal exponent = BigDecimal.ONE;
    BigDecimal sum = normTuple[normTuple.length - 1];
    for (int i = normTuple.length - 2; i >= 0; i--) {
      exponent = exponent.multiply(maxMax);
      BigDecimal val = normTuple[i].multiply(exponent);
      sum = sum.add(val);
    }
    return sum;
  }

  public static BigDecimal[] normTupleFromWeightedSum(AnalyzedSortSpec[] sortSpecs, BigDecimal weightedSum, int maxScale, int[] scale) {
    BigDecimal[] normTuple = new BigDecimal[sortSpecs.length];
//    BigDecimal maxMax = getNormMaxMax(sortSpecs);
    BigDecimal maxMax = BigDecimal.TEN.pow(maxScale);
    BigDecimal exponent = maxMax.pow(sortSpecs.length - 1);
    BigDecimal quotient, remainder;
    int i;
    for (i = 0; i < sortSpecs.length - 1; i++) {
      if (i < sortSpecs.length - 2) {
        quotient = weightedSum.divide(exponent, scale[i], BigDecimal.ROUND_DOWN);
      } else {
        quotient = weightedSum.divide(exponent, scale[i], BigDecimal.ROUND_UP);
      }
      remainder = weightedSum.subtract(quotient.multiply(exponent));
      normTuple[i] = quotient;
      weightedSum = remainder;
      exponent = exponent.divide(maxMax);
    }
    normTuple[i] = weightedSum;
    return normTuple;
  }

  public static List<Bucket> splitBucket(FreqHistogram histogram, AnalyzedSortSpec[] sortSpecs,
                                         Bucket bucket, Tuple interval) {
    Comparator<Tuple> comparator = histogram.getComparator();
    List<Bucket> splits = new ArrayList<>();

    BigDecimal[] bigInterval = HistogramUtil.normalize(sortSpecs, bucket.getBase(), true);

    if (bucket.getCount() == 1 ||
        isMinNormTuple(sortSpecs, bigInterval)) {
      splits.add(bucket);
    } else {
      long remaining = bucket.getCount();
      Tuple start = bucket.getStartKey();
      Tuple end = increment(sortSpecs, start, interval, 1);
      BigDecimal normalizedStart = weightedSum(sortSpecs, normalize(sortSpecs, start, false));
      BigDecimal totalDiff = weightedSum(sortSpecs, normalize(sortSpecs, bucket.getEndKey(), false)).subtract(normalizedStart);
      BigDecimal totalCount = BigDecimal.valueOf(bucket.getCount());

      while (comparator.compare(end, bucket.getEndKey()) < 0) {
        // count = totalCount * ( (end - start) / totalDiff )
        long count = roundToInteger(totalCount.multiply(
            weightedSum(sortSpecs, normalize(sortSpecs, end, false)).subtract(normalizedStart)
                .divide(totalDiff, DECIMAL128_HALF_UP))).longValue();
        splits.add(histogram.createBucket(new TupleRange(start, end, interval, comparator), count));
        start = end;
        try {
          end = HistogramUtil.increment(sortSpecs, start, interval, 1);
        } catch (TajoInternalError e) {
          // TODO
          break;
        }
        normalizedStart = weightedSum(sortSpecs, normalize(sortSpecs, start, false));
        remaining -= count;
      }

      if (!end.equals(bucket.getEndKey())) {
        // TODO: interval is invalid
        splits.add(histogram.createBucket(new TupleRange(start, bucket.getEndKey(), interval, comparator), remaining));
      }
    }

    return splits;
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
    // TODO: check the below line is valid
//    return val.multiply(sortSpec.getMax()).add(sortSpec.getMin());
    BigDecimal result = val.multiply(sortSpec.getMax(), DECIMAL128_HALF_UP);
    if (result.compareTo(sortSpec.getMax()) > 0) {
      throw new ArithmeticException("Overflow");
    }
    return result;
  }

  /**
   * Round half up the given value if its type is not real.
   *
   * @param type data type
   * @param val value
   * @return rounded value if its type is real.
   */
  private static BigDecimal roundIfNecessary(final Type type,
                                             BigDecimal val) {
    if (!type.equals(Type.FLOAT4) &&
        !type.equals(Type.FLOAT8)) {
      return roundToInteger(val);
    } else {
      return val;
    }
  }

  /**
   * Round half up the given value.
   *
   * @param val value to be rounded
   * @return rounded value
   */
  private static BigDecimal roundToInteger(BigDecimal val) {
    return val.setScale(0, RoundingMode.HALF_UP);
  }

  public static Datum denormalizeDatum(final AnalyzedSortSpec sortSpec,
                                       final BigDecimal val) {
    if (sortSpec.isNullFirst() && val.equals(BigDecimal.ZERO)) {
      return NullDatum.get();
    } else if (!sortSpec.isNullFirst() && val.equals(BigDecimal.ONE)) {
      return NullDatum.get();
    }

    BigDecimal denormalized = denormalizeVal(sortSpec, val);

    denormalized = roundIfNecessary(sortSpec.getType(), denormalized);

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
                                  final BigDecimal[] normTuple) {
    Tuple result = new VTuple(analyzedSpecs.length);
    for (int i = 0; i < analyzedSpecs.length; i++) {
      result.put(i, denormalizeDatum(analyzedSpecs[i], normTuple[i]));
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
  private static BigDecimal normalizeVal(final AnalyzedSortSpec sortSpec, final BigDecimal val) {
    BigDecimal result = val.divide(sortSpec.getMax(), DECIMAL128_HALF_UP);
    if (result.compareTo(BigDecimal.ZERO) < 0) {
      throw new ArithmeticException("Underflow: " + result + " should be larger than or equal to 0.");
    }
    if (result.compareTo(BigDecimal.ONE) > 0) {
      throw new ArithmeticException("Overflow: " + result + " should be smaller than or equal to 1.");
    }
    return result;
  }

  /**
   * Normalize the given datum into a real value in the range of [0, 1].
   *
   * @param sortSpec analyzed sort spec
   * @param val non-text datum
   * @return normalized value
   */
  private static BigDecimal normalizeDatum(final AnalyzedSortSpec sortSpec,
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
      return normalizeVal(sortSpec, normNumber);
    }
  }

  private static BigDecimal bytesToBigDecimal(final byte[] bytes,
                                              final int length,
                                              final boolean textPadFirst) {
    if (bytes.length == 0) {
      return BigDecimal.ZERO;
    } else {
      byte[] padded;
      if (textPadFirst) {
        padded = Bytes.padHead(bytes, bytes.length - length);
      } else {
        padded = Bytes.padTail(bytes, bytes.length - length);
      }
      return new BigDecimal(new BigInteger(padded));
    }
  }

  public static BigDecimal unicodeCharsToBigDecimal(final char[] unicodeChars,
                                                    final int length,
                                                    final boolean textPadFirst) {
    if (unicodeChars.length == 0) {
      return BigDecimal.ZERO;
    } else {
      char[] padded;
      if (textPadFirst) {
        padded = StringUtils.padHead(unicodeChars, length);
      } else {
        padded = StringUtils.padTail(unicodeChars, length);
      }
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
                                          final Datum val,
                                          final boolean textPadFirst) {
    if (val.isNull()) {
      return sortSpec.isNullFirst() ? BigDecimal.ZERO : BigDecimal.ONE;
    } else if (val.size() == 0) {
      return BigDecimal.ZERO;
    } else {
      if (sortSpec.isPureAscii()) {
        return normalizeVal(sortSpec, bytesToBigDecimal(val.asByteArray(), sortSpec.getMaxLength(), textPadFirst));
      } else {
        return normalizeVal(sortSpec, unicodeCharsToBigDecimal(val.asUnicodeChars(), sortSpec.getMaxLength(), textPadFirst));
      }
    }
  }

  public static BigDecimal[] normalizeTuple(AnalyzedSortSpec[] analyzedSpecs, Tuple tuple) {
    return normalize(analyzedSpecs, tuple, false);
  }

  public static BigDecimal[] normalizeInterval(AnalyzedSortSpec[] analyzedSpec, Tuple interval) {
    return normalize(analyzedSpec, interval, true);
  }

  /**
   * Normalize the given tuple into a real value.
   *
   * @param analyzedSpecs
   * @param tuple
   * @return
   */
  public static BigDecimal[] normalize(AnalyzedSortSpec[] analyzedSpecs, Tuple tuple, boolean textPadFirst) {
    BigDecimal[] result = new BigDecimal[analyzedSpecs.length];
    for (int i = analyzedSpecs.length - 1; i >= 0; i--) {
      if (analyzedSpecs[i].getType().equals(Type.TEXT)) {
        result[i] = normalizeText(analyzedSpecs[i], tuple.asDatum(i), textPadFirst);
      } else {
        result[i] = normalizeDatum(analyzedSpecs[i], tuple.asDatum(i));
      }
    }
    return result;
  }

  public static Tuple diff(final AnalyzedSortSpec[] sortSpecs,
                           final Tuple t1, final Tuple t2) {
    BigDecimal[] norm1 = normalize(sortSpecs, t1, false);
    BigDecimal[] norm2 = normalize(sortSpecs, t2, false);
    BigDecimal[] normDiff;
    if (compareNormTuples(norm1, norm2) > 0) {
      normDiff = increment(norm1, norm2, -1);
    } else {
      normDiff = increment(norm2, norm1, -1);
    }

    return denormalize(sortSpecs, normDiff);

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
    BigDecimal[] norm = HistogramUtil.normalize(sortSpecs, operand, false);
    BigDecimal[] interval = HistogramUtil.normalize(sortSpecs, baseTuple, true);
    BigDecimal[] incremented = HistogramUtil.increment(norm, interval, count);

    return HistogramUtil.denormalize(sortSpecs, incremented);

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
  }

  public static BigDecimal unicodeCharsToBigDecimal(char[] unicodeChars) {
    BigDecimal result = BigDecimal.ZERO;
    final BigDecimal base = BigDecimal.valueOf(TextDatum.UNICODE_CHAR_BITS_NUM);
    for (int i = unicodeChars.length-1; i >= 0; i--) {
      result = result.add(BigDecimal.valueOf(unicodeChars[i]).multiply(base.pow(unicodeChars.length-1-i, DECIMAL128_HALF_UP), DECIMAL128_HALF_UP));
    }
    return result;
  }

  public static char[] bigDecimalToUnicodeChars(BigDecimal val) {
    final BigDecimal base = BigDecimal.valueOf(TextDatum.UNICODE_CHAR_BITS_NUM);
    List<Character> characters = new ArrayList<>();
    BigDecimal divisor = val;
    while (divisor.compareTo(base) > 0) {
      BigDecimal[] quotiAndRemainder = divisor.divideAndRemainder(base, DECIMAL128_HALF_UP);
      divisor = quotiAndRemainder[0];
      characters.add(0, (char) quotiAndRemainder[1].intValue());
    }
    characters.add(0, (char) divisor.intValue());
    char[] chars = new char[characters.size()];
    for (int i = 0; i < characters.size(); i++) {
      chars[i] = characters.get(i);
    }
    return chars;
  }

  public static void refineToEquiDepth(final FreqHistogram histogram,
                                       final BigDecimal avgCard,
                                       final AnalyzedSortSpec[] sortSpecs) {
    List<Bucket> buckets = histogram.getSortedBuckets();
    Comparator<Tuple> comparator = histogram.getComparator();
    Bucket passed = null;

    // Refine from the last to the left direction.
    for (int i = buckets.size() - 1; i >= 0; i--) {
      Bucket current = buckets.get(i);
      // First add the passed range from the previous partition to the current one.
      if (passed != null) {
        current.merge(sortSpecs, passed);
        passed = null;
      }

//      if (!hasInfiniteDatum(current.getEndKey()) &&
//          !increment(sortSpecs, columnStatsList, current.getStartKey(), current.getBase(), 1, isPureAscii, maxLength).equals(current.getEndKey())) {
      if (current.getCount() > 1) {
        LOG.info("start refine from the last");
        int compare = BigDecimal.valueOf(current.getCount()).compareTo(avgCard);
        if (compare < 0) {
          // Take the lacking range from the next partition.
          long require = avgCard.subtract(BigDecimal.valueOf(current.getCount())).round(DECIMAL128_HALF_UP).longValue();
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
          long passAmount = BigDecimal.valueOf(current.getCount()).subtract(avgCard).round(DECIMAL128_HALF_UP).longValue();
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
        current.merge(sortSpecs, passed);
        passed = null;
      }
//      if (!hasInfiniteDatum(current.getEndKey()) &&
//          !increment(sortSpecs, columnStatsList, current.getStartKey(), current.getBase(), 1, isPureAscii, maxLength).equals(current.getEndKey())) {
      if (current.getCount() > 1) {
        LOG.info("start refine from the first");
        int compare = BigDecimal.valueOf(current.getCount()).compareTo(avgCard);
        if (compare < 0) {
          // Take the lacking range from the next partition.
          long require = avgCard.subtract(BigDecimal.valueOf(current.getCount())).round(DECIMAL128_HALF_UP).longValue();
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
          long passAmount = BigDecimal.valueOf(current.getCount()).subtract(avgCard).round(DECIMAL128_HALF_UP).longValue();
          Tuple newEnd = increment(sortSpecs, current.getEndKey(), current.getBase(), -1 * passAmount);
          passed = histogram.createBucket(new TupleRange(newEnd, current.getEndKey(), current.getKey().getBase(), comparator), passAmount);
          current.getKey().setEnd(newEnd);
          current.incCount(-1 * passAmount);
        }
      }
    }
  }

  private static class NormTupleComparator implements Comparator<BigDecimal[]> {

    @Override
    public int compare(BigDecimal[] o1, BigDecimal[] o2) {
      if (o1.length != o2.length)
        throw new ArrayIndexOutOfBoundsException("Two arrays have different lengths: " + o1.length + ", " + o2.length);

      for (int i = 0; i < o1.length; i++) {
        if (!o1[i].equals(o2[i])) {
          return o1[i].compareTo(o2[i]);
        }
      }
      return 0;
    }
  }

  private final static NormTupleComparator normTupleComparator = new NormTupleComparator();

  public static int compareNormTuples(BigDecimal[] t1, BigDecimal[] t2) {
    return normTupleComparator.compare(t1, t2);
  }

  public static BigDecimal[] increment(BigDecimal[] operand, BigDecimal[] interval, long count) {
    Preconditions.checkArgument(operand.length == interval.length);
    BigDecimal bigCount = BigDecimal.valueOf(count);
    BigDecimal[] carryAndRemainder = new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ZERO};
    BigDecimal[] incremented = new BigDecimal[operand.length];
    for (int i = operand.length - 1; i >= 0; i--) {
      // added = carry + operand + interval * count
      BigDecimal added = carryAndRemainder[0].add(operand[i].add(interval[i].multiply(bigCount, DECIMAL128_HALF_UP)));
      if (added.compareTo(BigDecimal.ONE) < 0) {
        carryAndRemainder[0] = BigDecimal.ZERO;
        incremented[i] = added;
      } else {
        carryAndRemainder = added.divideAndRemainder(BigDecimal.ONE, DECIMAL128_HALF_UP);
        incremented[i] = carryAndRemainder[1];
      }
    }
    return incremented;
  }
}
