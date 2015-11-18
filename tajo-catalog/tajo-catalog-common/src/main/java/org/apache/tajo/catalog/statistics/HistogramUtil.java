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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TupleRange;
import org.apache.tajo.catalog.statistics.FreqHistogram.Bucket;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;

public class HistogramUtil {

  private static final Log LOG = LogFactory.getLog(HistogramUtil.class);

//  public final static MathContext DECIMAL128_HALF_UP = new MathContext(34, RoundingMode.HALF_UP);
  public final static MathContext DECIMAL128_HALF_UP = new MathContext(68, RoundingMode.UP);

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
    Set<Datum>[] distValSets = new Set[sortSpecs.length];
    for (int i = 0; i < distValSets.length; i++) {
      distValSets[i] = new HashSet<>();
    }
//    double[] rounds = new double[sortSpecs.length];
//    Arrays.fill(rounds, 0);
    AnalyzedSortSpec[] analyzedSpecs = toAnalyzedSortSpecs(sortSpecs, columnStatses);
    BigDecimal sumCard = BigDecimal.ZERO;
    List<Bucket> buckets = histogram.getSortedBuckets();
//    Tuple first = buckets.get(0).getStartKey();
//    Tuple last = new VTuple(analyzedSpecs.length);
    for (Bucket bucket : buckets) {
      Tuple start = bucket.getStartKey();
      Tuple end = bucket.getEndKey();
//      for (int i = 0; i < sortSpecs.length; i++) {
//        if (end.asDatum(i).compareTo(start.asDatum(i)) < 0) {
//          rounds[i] += 1;
//        }
//        if (!end.isBlankOrNull(i)) {
//          last.put(i, end.asDatum(i));
//        }
//      }
      sumCard = sumCard.add(BigDecimal.valueOf(bucket.getCount()));
      for (int i = 0; i < sortSpecs.length; i++) {
        distValSets[i].add(start.asDatum(i));
        if (analyzedSpecs[i].getType().equals(Type.TEXT)) {
          boolean isCurrentPureAscii = StringUtils.isPureAscii(start.getText(i));
          if (analyzedSpecs[i].isPureAscii()) {
            analyzedSpecs[i].setPureAscii(isCurrentPureAscii);
          }
          if (isCurrentPureAscii) {
            analyzedSpecs[i].setMaxLength(Math.max(analyzedSpecs[i].getMaxLength(), start.getText(i).length()));
          } else {
            analyzedSpecs[i].setMaxLength(Math.max(analyzedSpecs[i].getMaxLength(), start.getUnicodeChars(i).length));
          }
        } else if (analyzedSpecs[i].getType().equals(Type.FLOAT4) || analyzedSpecs[i].getType().equals(Type.FLOAT8)) {
          double diff = (end.getFloat8(i) - start.getFloat8(i)) / bucket.getCount();
          if (diff > 0 && diff < analyzedSpecs[i].getMinInterval()) {
            analyzedSpecs[i].setMinInterval(diff);
          }
        }
      }
    }

//    for (int i = 0; i < sortSpecs.length; i++) {
//      // (max - last) + (first - min)
//      BigDecimal [] minMax = getMinMaxIncludeNull(analyzedSpecs[i]);
//      BigDecimal max = minMax[1], min = minMax[0];
//      BigDecimal transMax = max.subtract(min);
//
//      BigDecimal lastVal, firstVal;
//      if (sortSpecs[i].getSortKey().getDataType().getType().equals(Type.TEXT)) {
//        if (analyzedSpecs[i].isPureAscii()) {
//          firstVal = bytesToBigDecimal(first.getBytes(i), analyzedSpecs[i].getMaxLength(), true);
//          lastVal = bytesToBigDecimal(last.getBytes(i), analyzedSpecs[i].getMaxLength(), true);
//        } else {
//          firstVal = unicodeCharsToBigDecimal(first.getUnicodeChars(i), analyzedSpecs[i].getMaxLength(), true);
//          lastVal = unicodeCharsToBigDecimal(last.getUnicodeChars(i), analyzedSpecs[i].getMaxLength(), true);
//        }
//      } else {
//        firstVal = datumToBigDecimal(analyzedSpecs[i], first.asDatum(i));
//        lastVal = datumToBigDecimal(analyzedSpecs[i], last.asDatum(i));
//      }
//
//      if (rounds[i] == 0) {
//        rounds[i] = 1;
//      }
//      rounds[i] += max.subtract(lastVal).add(firstVal.subtract(min))
//          .divide(transMax, 64, BigDecimal.ROUND_HALF_UP).doubleValue();
//
//      BigDecimal distVal = transMax.multiply(BigDecimal.valueOf(rounds[i])).divide(sumCard, 128, BigDecimal.ROUND_HALF_UP);
//      analyzedSpecs[i].setNormMeanInterval(distVal.divide(transMax, 128, BigDecimal.ROUND_HALF_UP));
//    }

//    BigDecimal avgCard = sumCard.divide(BigDecimal.valueOf(buckets.size()), 128, BigDecimal.ROUND_HALF_UP);
//    for (int i = 0; i < distValSets.length; i++) {
//      long numDistVals = BigDecimal.valueOf(distValSets[i].size()).multiply(avgCard)
//          .divide(BigDecimal.valueOf(rounds[i]), 0, RoundingMode.HALF_UP).longValue();
//      analyzedSpecs[i].setNumDistVals(numDistVals);
//    }

    return analyzedSpecs;
  }

  public static Schema sortSpecsToSchema(SortSpec[] sortSpecs) {
    Schema schema = new Schema();
    for (SortSpec spec : sortSpecs) {
      schema.addColumn(spec.getSortKey());
    }

    return schema;
  }

  public static boolean isMinNormTuple(BigDecimal[] normTuple) {
    for (int i = 0; i < normTuple.length; i++) {
      if (!normTuple[i].equals(BigDecimal.ZERO)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get weighted sum of the given values.
   *
   * @param normTuple normalized values of a tuple
   * @param scale scales of normalized values
   * @return weighted sum of the values
   */
  protected static BigDecimal weightedSum(BigDecimal[] normTuple, int[] scale) {

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Note that arithmetic operations on weighted sum are not validated.
    // So, the below code is NOT valid.
    //
    // BigDecimal[] normTuple = normalizeTupleAsValue(sortSpecs, tuple);
    // BigDecimal[] incremented = increment(normTuple, normTuple, 1);
    //
    // int[] scales = maxScales(normTuple, normTuple);
    // BigDecimal sum = weightedSum(normTuple, scales);
    // sum *= 2;
    // BigDecimal[] fromSum = normTupleFromWeightedSum(sortSpecs, sum, scales);
    //
    // assertEquals(incremented, fromSum);
    //
    ///////////////////////////////////////////////////////////////////////////////////

    BigDecimal shiftAmount = BigDecimal.ONE;
    BigDecimal sum = BigDecimal.ZERO;
    for (int i = normTuple.length - 1; i >= 0; i--) {
      shiftAmount = shiftAmount.multiply(BigDecimal.TEN.pow(scale[i]));
      // The product of normTuple[i] and shiftAmount must be always an integer number, scale can be set as 0.
      BigDecimal val = normTuple[i].multiply(shiftAmount).setScale(0, BigDecimal.ROUND_DOWN);
      sum = sum.add(val);
    }
    return sum;
  }

  protected static BigDecimal[] normTupleFromWeightedSum(AnalyzedSortSpec[] sortSpecs, BigDecimal weightedSum,
                                                         int[] scale) {
    // TODO: need more tests
    BigDecimal[] normTuple = new BigDecimal[sortSpecs.length];
    BigDecimal quotient, remainder;
    BigDecimal shiftAmount = BigDecimal.ONE;
    int i;
    for (i = 0; i < scale.length; i++) {
      shiftAmount = shiftAmount.multiply(BigDecimal.TEN.pow(scale[i]));
    }
    for (i = 0; i < sortSpecs.length; i++) {
      quotient = weightedSum.divide(shiftAmount, scale[i], BigDecimal.ROUND_DOWN);
      remainder = weightedSum.subtract(quotient.multiply(shiftAmount)).setScale(0, BigDecimal.ROUND_DOWN);
      normTuple[i] = quotient;
      weightedSum = remainder;
      shiftAmount = shiftAmount.divideToIntegralValue(BigDecimal.TEN.pow(scale[i]));
    }
    return normTuple;
  }

  public static boolean splitable(AnalyzedSortSpec[] sortSpecs, Bucket bucket) {
    // TODO: many bugs. more tests are required.
    if (bucket.getCount() > 1) {
      BigDecimal startVal, endVal;
      Tuple start = bucket.getStartKey();
      Tuple end = bucket.getEndKey();
      for (int i = 0; i < sortSpecs.length; i++) {
        if (sortSpecs[i].getType().equals(Type.TEXT)) {
          if (sortSpecs[i].isPureAscii()) {
            startVal = bytesToBigDecimal(start.getTextBytes(i), sortSpecs[i].getMaxLength(), false);
            endVal = bytesToBigDecimal(end.getTextBytes(i), sortSpecs[i].getMaxLength(), false);
          } else {
            startVal = unicodeCharsToBigDecimal(start.getUnicodeChars(i), sortSpecs[i].getMaxLength(), false);
            endVal = unicodeCharsToBigDecimal(end.getUnicodeChars(i), sortSpecs[i].getMaxLength(), false);
          }
        } else {
          startVal = datumToBigDecimal(sortSpecs[i], start.asDatum(i));
          endVal = datumToBigDecimal(sortSpecs[i], end.asDatum(i));
        }

        if (endVal.subtract(startVal).compareTo(BigDecimal.valueOf(sortSpecs[i].getMinInterval())) > 0) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean mergeable(Comparator<Tuple> comparator, Bucket b1, Bucket b2) {
    if (comparator.compare(b1.getStartKey(), b2.getStartKey()) < 0) {
      return b1.getEndKey().equals(b2.getStartKey());
    } else if (comparator.compare(b1.getStartKey(), b2.getStartKey()) > 0) {
      return b1.getStartKey().equals(b2.getEndKey());
    } else {
      return false;
    }
  }

  protected static Bucket getSubBucket(FreqHistogram histogram,
                                       AnalyzedSortSpec[] analyzedSpecs,
                                       Bucket origin, Tuple start, Tuple end) {
    BigDecimal[] normStart = normalizeTupleAsValue(analyzedSpecs, origin.getStartKey());
    BigDecimal[] normEnd = normalizeTupleAsValue(analyzedSpecs, origin.getEndKey());
    BigDecimal[] normTotalInter = diff(analyzedSpecs, normEnd, normStart);

    BigDecimal[] normNewStart = normalizeTupleAsValue(analyzedSpecs, start);
    BigDecimal[] normNewEnd = normalizeTupleAsValue(analyzedSpecs, end);
    BigDecimal[] normSubInter = diff(analyzedSpecs, normNewEnd, normNewStart);

    int[] maxScales = HistogramUtil.maxScales(normTotalInter, normSubInter);
    BigDecimal totalRange = HistogramUtil.weightedSum(normTotalInter, maxScales);
    BigDecimal subRange = HistogramUtil.weightedSum(normSubInter, maxScales);
    BigDecimal totalAmount = BigDecimal.valueOf(origin.getCount());
    double newAmount = totalAmount.multiply(subRange)
        .divide(totalRange, 64, BigDecimal.ROUND_HALF_UP).doubleValue();

    return histogram.createBucket(new TupleRange(start, end, histogram.getComparator()), newAmount);
  }

  public static List<Bucket> splitBucket(FreqHistogram histogram, AnalyzedSortSpec[] sortSpecs,
                                         Bucket bucket, int splitNum) {
    Comparator<Tuple> comparator = histogram.getComparator();
    List<Bucket> splits = new ArrayList<>();

    Tuple start = bucket.getStartKey();
    Tuple end = bucket.getEndKey();
    Tuple diff = diff(sortSpecs, start, end);
//    BigDecimal[] normStart = normalizeTupleAsValue(sortSpecs, start);
//    BigDecimal[] normEnd = normalizeTupleAsValue(sortSpecs, end);
    BigDecimal[] normDiff = normalizeTupleAsVector(sortSpecs, diff);
    int[] maxScales = maxScales(normDiff, normDiff);


    BigDecimal diffVal = weightedSum(normDiff, maxScales);
    BigDecimal newInter = diffVal.divide(BigDecimal.valueOf(splitNum), 128, BigDecimal.ROUND_HALF_UP);
    BigDecimal[] newNormInter = normTupleFromWeightedSum(sortSpecs, newInter, maxScales);
    Tuple interval = denormalizeAsVector(sortSpecs, newNormInter);

//    Tuple interval = denormalizeAsVector(sortSpecs, normInter);

//    BigDecimal[] normInterval = normalizeTupleAsVector(sortSpecs, bucket.getInterval());
//
//    if (bucket.getCount() == 1 || isMinNormTuple(normInterval)) {
//      splits.add(bucket);
//    } else {
//      long newCard = HistogramUtil.diff(sortSpecs, interval, bucket.getStartKey(), bucket.getEndKey());
//      long origCard = bucket.getCount();
//
//      // newCard must be smaller than origCard
//      Preconditions.checkState(newCard <= origCard);
//
//      long desire = Math.round((double)origCard / newCard);
//      long remaining = origCard;
//      Tuple start = bucket.getStartKey(), end;
//      while (remaining > desire) {
//        end = increment(sortSpecs, start, interval, desire);
//        splits.add(histogram.createBucket(new TupleRange(start, end, interval, comparator), desire));
//        remaining -= desire;
//      }
//
//      if (remaining > 0) {e
//        end = increment(sortSpecs, start, interval, remaining);
//        splits.add(histogram.createBucket(new TupleRange(start, end, interval, comparator), remaining));
//      }
//    }

    return splits;
  }

  protected static int[] maxScales(BigDecimal[]...tuples) {
    int[] maxScales = new int[tuples[0].length];
    Arrays.fill(maxScales, 0);

    for (BigDecimal[] tuple : tuples) {
      for (int i = 0; i < maxScales.length; i++) {
        if (maxScales[i] < tuple[i].scale()) {
          maxScales[i] = tuple[i].scale();
        }
      }
    }

    return maxScales;
  }

  public static Datum getLastValue(SortSpec[] sortSpecs, List<ColumnStats> columnStatsList, int i) {
    return columnStatsList.get(i).hasNullValue() ?
        sortSpecs[i].isNullsFirst() ? columnStatsList.get(i).getMaxValue() : NullDatum.get()
        : columnStatsList.get(i).getMaxValue();
  }

  public static Datum getFirstValue(SortSpec[] sortSpecs, List<ColumnStats> columnStatsList, int i) {
    return columnStatsList.get(i).hasNullValue() ?
        sortSpecs[i].isNullsFirst() ? NullDatum.get() : columnStatsList.get(i).getMinValue()
        : columnStatsList.get(i).getMinValue();
  }

  protected static BigDecimal[] getMinMaxIncludeNull(final AnalyzedSortSpec sortSpec) {
    BigDecimal min, max;
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
          max = bytesToBigDecimal(sortSpec.getMaxValue().asByteArray(), sortSpec.getMaxLength(), false);
          min = bytesToBigDecimal(sortSpec.getMinValue().asByteArray(), sortSpec.getMaxLength(), false);
        } else {
          max = unicodeCharsToBigDecimal(sortSpec.getMaxValue().asUnicodeChars(), sortSpec.getMaxLength(), false);
          min = unicodeCharsToBigDecimal(sortSpec.getMinValue().asUnicodeChars(), sortSpec.getMaxLength(), false);
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
      if (sortSpec.isNullsFirst()) {
        min = min.subtract(BigDecimal.ONE);
      } else {
        max = max.add(BigDecimal.ONE);
      }
    }

    max = max.add(BigDecimal.ONE); // Make the max value exclusive
    return new BigDecimal[] {min, max};
  }

  protected static BigDecimal denormalizeVal(final AnalyzedSortSpec sortSpec,
                                             final BigDecimal val,
                                             final boolean isValue) {
    // TODO: check the below line is valid
//    return val.multiply(sortSpec.getMax()).add(sortSpec.getMin());
    BigDecimal result = val.multiply(sortSpec.getTransMax());
    if (isValue) {
      result = result.add(sortSpec.getMin());
    }
    if (isValue) {
      if (result.compareTo(sortSpec.getMax()) > 0) {
        throw new ArithmeticException("Overflow");
      } else if (result.compareTo(sortSpec.getMin()) < 0) {
        throw new ArithmeticException("Underflow");
      }
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
  protected static BigDecimal roundToInteger(BigDecimal val) {
    return val.setScale(0, RoundingMode.HALF_UP);
  }

  protected static Datum denormalizeDatum(final AnalyzedSortSpec sortSpec,
                                          final BigDecimal val,
                                          final boolean isValue) {
    if (sortSpec.isNullsFirst() && val.equals(BigDecimal.ZERO)) {
      return NullDatum.get();
    } else if (!sortSpec.isNullsFirst() && val.equals(BigDecimal.ONE)) {
      return NullDatum.get();
    }

    BigDecimal denormalized = denormalizeVal(sortSpec, val, isValue);

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
        String text;
        if (sortSpec.isPureAscii()) {
          text = new String(denormalized.toBigInteger().toByteArray());
        } else {
          text = new String(bigDecimalToUnicodeChars(denormalized));
        }
        return DatumFactory.createText(text);
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

  public static Tuple denormalizeAsValue(final AnalyzedSortSpec[] analyzedSpecs,
                                         final BigDecimal[] normTuple) {
    return denormalize(analyzedSpecs, normTuple, true);
  }

  public static Tuple denormalizeAsVector(final AnalyzedSortSpec[] analyzedSpecs,
                                          final BigDecimal[] normTuple) {
    return denormalize(analyzedSpecs, normTuple, false);
  }

  protected static Tuple denormalize(final AnalyzedSortSpec[] analyzedSpecs,
                                  final BigDecimal[] normTuple,
                                  final boolean isValue) {
    Tuple result = new VTuple(analyzedSpecs.length);
    for (int i = 0; i < analyzedSpecs.length; i++) {
      result.put(i, denormalizeDatum(analyzedSpecs[i], normTuple[i], isValue));
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
  private static BigDecimal normalizeVal(final AnalyzedSortSpec sortSpec,
                                         BigDecimal val,
                                         final boolean isValue) {
    if (isValue) {
      val = val.subtract(sortSpec.getMin());
    }
    BigDecimal result = val.divide(sortSpec.getTransMax(), 128, RoundingMode.HALF_UP);
    if (isValue) {
      if (result.compareTo(BigDecimal.ZERO) < 0) {
        throw new ArithmeticException("Underflow: " + result + " should be larger than or equal to 0");
//      } else if (result.compareTo(BigDecimal.ONE) > 0) {
      } else if (result.compareTo(sortSpec.getNormTransMax()) > 0) {
        throw new ArithmeticException("Overflow: " + result + " should be smaller than or equal to 1");
      }
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
                                           final Datum val,
                                           final boolean isValue) {
    if (val.isNull()) {
      return sortSpec.isNullsFirst() ? BigDecimal.ZERO : BigDecimal.ONE;
    } else {
      BigDecimal normNumber = datumToBigDecimal(sortSpec, val);
      return normalizeVal(sortSpec, normNumber, isValue);
    }
  }

  private static BigDecimal datumToBigDecimal(final AnalyzedSortSpec sortSpec, final Datum val) {
    if (val.isNull()) {
      return sortSpec.isNullsFirst() ? BigDecimal.ZERO : BigDecimal.ONE;
    } else {
      BigDecimal normNumber;
      switch (sortSpec.getType()) {
        case BOOLEAN:
          // TODO
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
      return normNumber;
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
        padded = Bytes.padHead(bytes, length - bytes.length);
      } else {
        padded = Bytes.padTail(bytes, length - bytes.length);
      }
      return new BigDecimal(new BigInteger(padded));
    }
  }

  public static BigDecimal unicodeCharsToBigDecimal(final char[] unicodeChars,
                                                    final int length,
                                                    boolean textPadFirst) {
    if (unicodeChars.length == 0) {
      return BigDecimal.ZERO;
    } else {
      char[] padded;
//      textPadFirst = false;
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
                                          final boolean textPadFirst,
                                          final boolean isValue) {
    if (val.isNull()) {
      return sortSpec.isNullsFirst() ? BigDecimal.ZERO : BigDecimal.ONE;
    } else if (val.size() == 0) {
      return BigDecimal.ZERO;
    } else {
      if (sortSpec.isPureAscii()) {
        return normalizeVal(sortSpec, bytesToBigDecimal(val.asByteArray(), sortSpec.getMaxLength(), textPadFirst), isValue);
      } else {
        return normalizeVal(sortSpec, unicodeCharsToBigDecimal(val.asUnicodeChars(), sortSpec.getMaxLength(), textPadFirst), isValue);
      }
    }
  }

  public static BigDecimal[] normalizeTupleAsValue(AnalyzedSortSpec[] analyzedSpecs, Tuple tuple) {
    return normalize(analyzedSpecs, tuple, false, true);
  }

  public static BigDecimal[] normalizeTupleAsVector(AnalyzedSortSpec[] analyzedSpec, Tuple interval) {
    return normalize(analyzedSpec, interval, true, false);
  }

  /**
   * Normalize the given tuple into a real value.
   *
   * @param analyzedSpecs
   * @param tuple
   * @return
   */
  protected static BigDecimal[] normalize(final AnalyzedSortSpec[] analyzedSpecs,
                                       final Tuple tuple,
                                       final boolean textPadFirst,
                                       final boolean isValue) {
    BigDecimal[] result = new BigDecimal[analyzedSpecs.length];
    for (int i = analyzedSpecs.length - 1; i >= 0; i--) {
      if (analyzedSpecs[i].getType().equals(Type.TEXT)) {
        result[i] = normalizeText(analyzedSpecs[i], tuple.asDatum(i), textPadFirst, isValue);
      } else {
        result[i] = normalizeDatum(analyzedSpecs[i], tuple.asDatum(i), isValue);
      }
    }
    return result;
  }

  public static long diff(final AnalyzedSortSpec[] sortSpecs, final Tuple interval,
                          final Tuple t1, final Tuple t2) {
    BigDecimal[] norm1 = normalizeTupleAsValue(sortSpecs, t1);
    BigDecimal[] norm2 = normalizeTupleAsValue(sortSpecs, t2);
    BigDecimal[] normInter = normalizeTupleAsVector(sortSpecs, interval);
    BigDecimal[] large, small;
    if (compareNormTuples(norm1, norm2) > 0) {
      large = norm1;
      small = norm2;
    } else {
      large = norm2;
      small = norm1;
    }

    long i;
    for (i = 0; compareNormTuples(small, large) < 0; i++) {
      small = increment(sortSpecs, small, normInter, 1);
    }

    // TODO: handle the case of (small > large)
    return i;
  }

  /**
   * Return approximated difference between two tuples
   *
   * @param sortSpecs
   * @param t1
   * @param t2
   * @return
   */
  public static Tuple diff(final AnalyzedSortSpec[] sortSpecs,
                           final Tuple t1, final Tuple t2) {
    BigDecimal[] n1 = normalizeTupleAsValue(sortSpecs, t1);
    BigDecimal[] n2 = normalizeTupleAsValue(sortSpecs, t2);
    BigDecimal[] normDiff = diff(sortSpecs, n1, n2);
    return denormalizeAsVector(sortSpecs, normDiff);
  }

  public static Tuple diffVector(final AnalyzedSortSpec[] sortSpecs,
                                 final Tuple t1, final Tuple t2) {
    BigDecimal[] n1 = normalizeTupleAsVector(sortSpecs, t1);
    BigDecimal[] n2 = normalizeTupleAsVector(sortSpecs, t2);
    BigDecimal[] normDiff = diff(sortSpecs, n1, n2);
    return denormalizeAsVector(sortSpecs, normDiff);
  }

  protected static BigDecimal[] diff(final AnalyzedSortSpec[] sortSpecs,
                                     final BigDecimal[] n1, final BigDecimal[] n2) {
    if (normTupleComparator.compare(n1, n2) < 0) {
      return approxDiff(sortSpecs, n2, n1);
    } else {
      return approxDiff(sortSpecs, n1, n2);
    }
  }

  protected static BigDecimal[] approxDiff(final AnalyzedSortSpec[] sortSpecs,
                                           final BigDecimal[] large, final BigDecimal[] small) {
    BigDecimal [] diff = new BigDecimal[sortSpecs.length];
    for (int i = 0; i < sortSpecs.length; i++) {
      if (large[i].compareTo(small[i]) >= 0) {
        diff[i] = large[i].subtract(small[i]);
      } else {
        diff[i] = large[i].add(sortSpecs[i].getNormTransMax()).subtract(small[i]);
      }
    }
    return diff;
  }

  /**
   * Increment the tuple with the amount of the product of <code>count</code> and <code>baseTuple</code>.
   * If the <code>count</code> is a negative value, it will work as decrement.
   *
   * @param sortSpecs an array of sort specifications
   * @param value tuple to be incremented
   * @param vector increment amount
   * @return incremented tuple
   */
  public static Tuple incrementValue(final AnalyzedSortSpec[] sortSpecs, final Tuple value, final Tuple vector) {
    BigDecimal[] norm = normalizeTupleAsValue(sortSpecs, value);
    BigDecimal[] interval = normalizeTupleAsVector(sortSpecs, vector);
    BigDecimal[] incremented = increment(sortSpecs, norm, interval, 1);

    return denormalizeAsValue(sortSpecs, incremented);
  }

  public static Tuple incrementValue(final AnalyzedSortSpec[] sortSpecs, final Tuple value, final Tuple vector, double amount) {
    BigDecimal[] norm = normalizeTupleAsValue(sortSpecs, value);
    BigDecimal[] interval = normalizeTupleAsVector(sortSpecs, vector);
    BigDecimal[] incremented = increment(sortSpecs, norm, interval, amount);

    return denormalizeAsValue(sortSpecs, incremented);
  }

  public static Tuple incrementVector(AnalyzedSortSpec[] sortSpecs, final Tuple vector, final Tuple baseTuple, double amount) {
    BigDecimal[] norm = normalizeTupleAsVector(sortSpecs, vector);
    BigDecimal[] interval = normalizeTupleAsVector(sortSpecs, baseTuple);
    BigDecimal[] incremented = increment(sortSpecs, norm, interval, amount);

    return denormalizeAsVector(sortSpecs, incremented);
  }

  public static BigDecimal unicodeCharsToBigDecimal(char[] unicodeChars) {
    BigDecimal result = BigDecimal.ZERO;
    final BigDecimal base = BigDecimal.valueOf(TextDatum.UNICODE_CHAR_BITS_NUM);
    for (int i = unicodeChars.length-1; i >= 0; i--) {
      if (unicodeChars[i] < 0) {
        throw new RuntimeException(unicodeChars[i] + " is negative");
      }
      result = result.add(BigDecimal.valueOf(unicodeChars[i]).multiply(base.pow(unicodeChars.length-1-i)));
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
//    List<Bucket> buckets = histogram.getSortedBuckets();
//    Comparator<Tuple> comparator = histogram.getComparator();
//    Bucket passed = null;
//
//    // Refine from the last to the left direction.
//    for (int i = buckets.size() - 1; i >= 0; i--) {
//      Bucket current = buckets.get(i);
//      // First add the passed range from the previous partition to the current one.
//      if (passed != null) {
//        current.merge(sortSpecs, passed);
//        passed = null;
//      }
//
//      if (current.getCount() > 1) {
//        int compare = BigDecimal.valueOf(current.getCount()).compareTo(avgCard);
//        if (compare < 0) {
//          // Take the lacking range from the next partition.
//          long require = roundToInteger(avgCard.subtract(BigDecimal.valueOf(current.getCount()))).longValue();
//          int direction = sortSpecs[i].isAscending() ? -1 : 1;
//          for (int j = i - 1; j >= 0 && require > 0; j--) {
//            Bucket nextBucket = buckets.get(j);
//            long takeAmount = require < nextBucket.getCount() ? require : nextBucket.getCount();
//            Tuple newStart = increment(sortSpecs, current.getStartKey(), nextBucket.getInterval(), direction * takeAmount);
//            current.getKey().setStart(newStart);
//            current.incCount(takeAmount);
//            nextBucket.getKey().setEnd(newStart);
//            nextBucket.incCount(-1 * takeAmount);
//            require -= takeAmount;
//          }
//
//        } else if (compare > 0) {
//          // Pass the remaining range to the next partition.
//          long passAmount = roundToInteger(BigDecimal.valueOf(current.getCount()).subtract(avgCard)).longValue();
//          int direction = sortSpecs[i].isAscending() ? -1 : 1;
//          Tuple newStart = increment(sortSpecs, current.getStartKey(), current.getInterval(), direction * passAmount);
//          passed = histogram.createBucket(new TupleRange(current.getStartKey(), newStart, current.getInterval(), comparator),
//              passAmount);
//          current.getKey().setStart(newStart);
//          current.incCount(-1 * passAmount);
//        }
//      }
//    }
//
//    // Refine from the first to the right direction
//    for (int i = 0; i < buckets.size(); i++) {
//      Bucket current = buckets.get(i);
//      // First add the passed range from the previous partition to the current one.
//      if (passed != null) {
//        current.merge(sortSpecs, passed);
//        passed = null;
//      }
//      if (current.getCount() > 1) {
//        int compare = BigDecimal.valueOf(current.getCount()).compareTo(avgCard);
//        if (compare < 0) {
//          // Take the lacking range from the next partition.
//          long require = roundToInteger(avgCard.subtract(BigDecimal.valueOf(current.getCount()))).longValue();
//          int direction = sortSpecs[i].isAscending() ? 1 : -1;
//          for (int j = i + 1; j < buckets.size() && require > 0; j++) {
//            Bucket nextBucket = buckets.get(j);
//            long takeAmount = require < nextBucket.getCount() ? require : nextBucket.getCount();
//            Tuple newEnd = increment(sortSpecs, current.getEndKey(), current.getInterval(), direction * takeAmount);
//            current.getKey().setEnd(newEnd);
//            current.incCount(takeAmount);
//            nextBucket.getKey().setStart(newEnd);
//            nextBucket.incCount(-1 * takeAmount);
//            require -= takeAmount;
//          }
//
//        } else if (compare > 0) {
//          // Pass the remaining range to the next partition.
//          long passAmount = roundToInteger(BigDecimal.valueOf(current.getCount()).subtract(avgCard)).longValue();
//          int direction = sortSpecs[i].isAscending() ? 1 : -1;
//          Tuple newEnd = increment(sortSpecs, current.getEndKey(), current.getInterval(), direction * passAmount);
//          passed = histogram.createBucket(new TupleRange(newEnd, current.getEndKey(), current.getKey().getInterval(), comparator), passAmount);
//          current.getKey().setEnd(newEnd);
//          current.incCount(-1 * passAmount);
//        }
//      }
//    }
//
//    // TODO: if there are remaining passed bucket,
//    if (passed != null && passed.getCount() > 0) {
//      Bucket lastBucket = buckets.get(buckets.size() - 1);
//      lastBucket.merge(sortSpecs, passed);
//    }
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

  protected static BigDecimal[] increment(AnalyzedSortSpec[] sortSpecs, BigDecimal[] operand, BigDecimal[] interval,
                                          double amount) {
//    Preconditions.checkArgument(operand.length == interval.length);
    BigDecimal bigCount = BigDecimal.valueOf(amount);
    BigDecimal carry = BigDecimal.ZERO;
    BigDecimal[] incremented = new BigDecimal[operand.length];
    for (int i = operand.length - 1; i >= 0; i--) {
      // TODO: the below round should be improved
//      BigDecimal added = operand[i].add(carry.add(bigCount).multiply(sortSpecs[i].getNormMeanInterval()))
//          .setScale(127, BigDecimal.ROUND_HALF_UP);

      // added = operand + carry * meanInterval + interval * count
      BigDecimal added = operand[i]
          .add(carry.multiply(sortSpecs[i].getNormMinInterval()))
          .add(bigCount.multiply(interval[i]));

      // Calculate carry and remainder
      if (added.compareTo(BigDecimal.ZERO) < 0) {
//        carry = added.abs().setScale(0, BigDecimal.ROUND_CEILING);
//        int maxScale = added.scale() > sortSpecs[i].getNormTransMax().scale() ?
//            added.scale() : sortSpecs[i].getNormTransMax().scale();
//        carry = added.abs().divide(sortSpecs[i].getTransMax(), maxScale, BigDecimal.ROUND_HALF_UP)
//            .setScale(0, BigDecimal.ROUND_CEILING);
        carry = BigDecimal.valueOf(added.abs().doubleValue() / sortSpecs[i].getNormTransMax().doubleValue())
            .setScale(0, BigDecimal.ROUND_CEILING);
        assert carry.compareTo(added) >= 0;
        incremented[i] = carry.add(added);
        carry = carry.negate();
      } else if (added.compareTo(sortSpecs[i].getNormTransMax()) < 0) {
        carry = BigDecimal.ZERO;
        incremented[i] = added;
      } else {
//        carryAndRemainder[0] = added.setScale(0, BigDecimal.ROUND_FLOOR);
//        int maxScale = added.scale() > sortSpecs[i].getNormTransMax().scale() ?
//            added.scale() : sortSpecs[i].getNormTransMax().scale();
//        carry = added.divide(sortSpecs[i].getTransMax(), maxScale, BigDecimal.ROUND_HALF_UP)
//            .setScale(0, BigDecimal.ROUND_FLOOR);
        carry = BigDecimal.valueOf((long) (added.doubleValue() / sortSpecs[i].getNormTransMax().doubleValue()), 0);
        incremented[i] = added.subtract(carry);
      }
    }
    if (!carry.equals(BigDecimal.ZERO)) {
      if (carry.compareTo(BigDecimal.ZERO) < 0) {
        throw new ArithmeticException("Underflow");
      } else {
        throw new ArithmeticException("Overflow");
      }
    }
    return incremented;
  }

//  protected static Tuple getMeanInterval(AnalyzedSortSpec[] sortSpecs, Tuple interval1, Tuple interval2) {
//    BigDecimal[] n1 = normalizeTupleAsVector(sortSpecs, interval1);
//    BigDecimal[] n2 = normalizeTupleAsVector(sortSpecs, interval2);
//    BigDecimal[] large, small, normMeanInterval = new BigDecimal[sortSpecs.length];
//    if (compareNormTuples(n1, n2) < 0) {
//      large = n2;
//      small = n1;
//    } else {
//      large = n1;
//      small = n2;
//    }
//
//    BigDecimal carry = BigDecimal.ZERO;
//    int[] scales = HistogramUtil.maxScales(n1, n2);
//    for (int i = sortSpecs.length - 1; i >= 0; i--) {
//      BigDecimal carryAdded;
//      if (!carry.equals(BigDecimal.ZERO)) {
//        carryAdded = carry.divide(sortSpecs[i].getTransMax(), scales[i], BigDecimal.ROUND_HALF_UP)
//            .add(large[i]);
//      } else {
//        carryAdded = large[i];
//      }
//
//      if (carryAdded.compareTo(small[i]) >= 0) {
//        normMeanInterval[i] = carryAdded.add(small[i])
//            .divide(BigDecimal.valueOf(2), scales[i], BigDecimal.ROUND_HALF_UP);
//        carry = BigDecimal.ZERO;
//      } else {
//        normMeanInterval[i] = carryAdded.add(BigDecimal.ONE).add(small[i])
//            .divide(BigDecimal.valueOf(2), scales[i], BigDecimal.ROUND_HALF_UP);
//        carry = BigDecimal.ONE.negate();
//      }
//    }
//
//
////    BigDecimal s1 = HistogramUtil.weightedSum(n1, scales);
////    BigDecimal s2 = HistogramUtil.weightedSum(n2, scales);
////    int maxScale = 0;
////    for (int scale : scales) {
////      maxScale = maxScale < scale ? scale : maxScale;
////    }
////
////    BigDecimal mean = s1.add(s2).divide(BigDecimal.valueOf(2), maxScale, BigDecimal.ROUND_HALF_UP);
////    BigDecimal[] normMeanInterval = HistogramUtil.normTupleFromWeightedSum(sortSpecs, mean, scales);
//    return denormalizeAsVector(sortSpecs, normMeanInterval);
//  }
}
