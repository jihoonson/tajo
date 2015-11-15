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
import org.apache.tajo.catalog.proto.CatalogProtos.FreqHistogramProto;
import org.apache.tajo.catalog.statistics.FreqHistogram.Bucket;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.zookeeper.KeeperException.UnimplementedException;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHistogramUtil {

  private static Schema schema;
  private static SortSpec[] sortSpecs;
  private static AnalyzedSortSpec[] analyzedSpecs;
  private static FreqHistogram histogram;
  private static Tuple totalBase;
  private static List<ColumnStats> columnStatsList;
  private static BigDecimal totalCount;

  private static BitSet FALSE_SET;
  private static BitSet TRUE_SET;
  private static TupleComparator comparator;

  @Before
  public void setup() {
    schema = new Schema();
    schema.addColumn(new Column("col1", Type.FLOAT8));
    schema.addColumn(new Column("col2", Type.INT8));
    schema.addColumn(new Column("col3", Type.TEXT));
    schema.addColumn(new Column("col4", Type.TIMESTAMP));

    FALSE_SET = new BitSet(schema.size());
    TRUE_SET = new BitSet(schema.size());
    TRUE_SET.set(0, schema.size());

    totalBase = getVTuple(DatumFactory.createFloat8(0.5), DatumFactory.createInt8(10),
        DatumFactory.createText(new String(new char[] {1176})),
        DatumFactory.createTimestampDatumWithJavaMillis(1000));

    columnStatsList = new ArrayList<>(schema.size());
    for (Column column : schema.getAllColumns()) {
      columnStatsList.add(new ColumnStats(column));
    }
  }

  private void prepareHistogram(BitSet asc, BitSet nullFirst) {
    sortSpecs = new SortSpec[schema.size()];
    for (int i = 0; i < sortSpecs.length; i++) {
      sortSpecs[i] = new SortSpec(schema.getColumn(i), asc.get(i), nullFirst.get(i));
    }
    comparator = new BaseTupleComparator(schema, sortSpecs);

    histogram = new FreqHistogram(sortSpecs);

    long card = 0;
    Tuple start = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(10));
    Datum[] minDatums = new Datum[schema.size()];
    for (int i = 0; i < minDatums.length; i++) {
      minDatums[i] = start.asDatum(i);
    }
    Tuple end;
    if (nullFirst.cardinality() > 0) {
      Tuple tuple = getNullIncludeVTuple(nullFirst, start, false);
      histogram.updateBucket(tuple, start, 50);
      for (int i = 0; i < minDatums.length; i++) {
        if (!tuple.isBlankOrNull(i)) {
          minDatums[i] = tuple.asDatum(i);
        }
      }
      card += 50;
    }
    for (int i = 0; i < columnStatsList.size(); i++) {
      columnStatsList.get(i).setMinValue(minDatums[i]);
    }
    columnStatsList.get(0).setMaxValue(DatumFactory.createFloat8(10000000d));
    columnStatsList.get(1).setMaxValue(DatumFactory.createInt8(1000));
    columnStatsList.get(2).setMaxValue(DatumFactory.createText("하"));
    columnStatsList.get(3).setMaxValue(DatumFactory.createTimestampDatumWithJavaMillis(1000000));

    analyzedSpecs = HistogramUtil.toAnalyzedSortSpecs(sortSpecs, columnStatsList);
    analyzedSpecs[2].setPureAscii(false);
    analyzedSpecs[2].setMaxLength(1);

    for (int i = 0; i < 20; i++) {
      long count = (i + 1) * 10;
      end = HistogramUtil.incrementValue(analyzedSpecs, start, totalBase, count);
      histogram.updateBucket(start, end, count);
      start = end;
      card += count;
    }
    if (nullFirst.cardinality() < schema.size()) {
      nullFirst.flip(0, schema.size());
      Tuple tuple = getNullIncludeVTuple(nullFirst, start, true);
      histogram.updateBucket(start, tuple, 50);
      card += 50;
    }
    totalCount = BigDecimal.valueOf(card);
  }

  private static Tuple getVTuple(Datum... datums) {
    return new VTuple(datums);
  }

  private static Tuple getNullIncludeVTuple(BitSet nullFlags, Tuple minOrMax, boolean isMin) {
    Tuple nullIncludeTuple = new VTuple(schema.size());
    for (int i = 0; i < schema.size(); i++) {
      if (nullFlags.get(i)) {
        nullIncludeTuple.put(i, NullDatum.get());
      } else {
        if (isMin) {
          switch (schema.getColumn(i).getDataType().getType()) {
            case FLOAT8:
              nullIncludeTuple.put(i, DatumFactory.createFloat8(minOrMax.getFloat8(i) + totalBase.getFloat8(i)));
              break;
            case INT8:
              nullIncludeTuple.put(i, DatumFactory.createInt8(minOrMax.getInt8(i) + totalBase.getInt8(i)));
              break;
            case TEXT:
              BigDecimal base = HistogramUtil.unicodeCharsToBigDecimal(minOrMax.getUnicodeChars(i), analyzedSpecs[i].getMaxLength(), false);
              BigDecimal add = HistogramUtil.unicodeCharsToBigDecimal(totalBase.getUnicodeChars(i), analyzedSpecs[i].getMaxLength(), false);
              nullIncludeTuple.put(i, DatumFactory.createText(Convert.chars2utf(HistogramUtil.bigDecimalToUnicodeChars(base.add(add)))));
              break;
            case TIMESTAMP:
              nullIncludeTuple.put(i, DatumFactory.createTimestampDatumWithJavaMillis(((TimestampDatum)minOrMax.asDatum(i)).getJavaTimestamp() + ((TimestampDatum)totalBase.asDatum(i)).getJavaTimestamp()));
              break;
            default:
              throw new RuntimeException(new UnimplementedException());
          }
        } else {
          switch (schema.getColumn(i).getDataType().getType()) {
            case FLOAT8:
              nullIncludeTuple.put(i, DatumFactory.createFloat8(minOrMax.getFloat8(i) - totalBase.getFloat8(i)));
              break;
            case INT8:
              nullIncludeTuple.put(i, DatumFactory.createInt8(minOrMax.getInt8(i) - totalBase.getInt8(i)));
              break;
            case TEXT:
              BigDecimal base = HistogramUtil.unicodeCharsToBigDecimal(minOrMax.getUnicodeChars(i));
              BigDecimal add = HistogramUtil.unicodeCharsToBigDecimal(totalBase.getUnicodeChars(i));
              nullIncludeTuple.put(i, DatumFactory.createText(Convert.chars2utf(HistogramUtil.bigDecimalToUnicodeChars(base.subtract(add)))));
              break;
            case TIMESTAMP:
              nullIncludeTuple.put(i, DatumFactory.createTimestampDatumWithJavaMillis(((TimestampDatum)minOrMax.asDatum(i)).getJavaTimestamp() - ((TimestampDatum)totalBase.asDatum(i)).getJavaTimestamp()));
              break;
            default:
              throw new RuntimeException(new UnimplementedException());
          }
        }
      }
    }
    return nullIncludeTuple;
  }

  @Test
  public void testPBSerde() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    FreqHistogramProto proto = histogram.getProto();
    FreqHistogram deserialized = new FreqHistogram(proto);
    assertEquals(histogram, deserialized);
  }

  @Test
  public void testIncrement() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(10));
    Tuple result = HistogramUtil.incrementValue(analyzedSpecs, tuple, totalBase, 10);

    assertEquals(5.1, result.getFloat8(0), 0.0001);
    assertEquals(112, result.getInt8(1));
    assertEquals("낗", result.getText(2));
    assertEquals("1970-01-01 00:00:10.01", result.getTimeDate(3).toString());

    result = HistogramUtil.incrementValue(analyzedSpecs, result, totalBase, -10);

    assertEquals(0.1, result.getFloat8(0), 0.0001);
    assertEquals(2, result.getInt8(1));
    assertEquals("가", result.getText(2));
    assertEquals("1970-01-01 00:00:00.01", result.getTimeDate(3).toString());
  }

  @Test
  public void testIncrement2() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(969.1), DatumFactory.createInt8(21),
        DatumFactory.createText("다"), DatumFactory.createTimestamp("1970-01-01 00:15:00.019"));
    Tuple incremented = HistogramUtil.incrementValue(analyzedSpecs, tuple, totalBase, 3);

    assertEquals(970.6, incremented.getFloat8(0), 0.000001);
    assertEquals(51, incremented.getInt8(1));
    assertEquals("사", incremented.getText(2));
    assertEquals("1970-01-01 00:15:03.019", incremented.getTimeDate(3).toString());

    Tuple result = HistogramUtil.incrementValue(analyzedSpecs, incremented, totalBase, -3);

    assertEquals(969.1, result.getFloat8(0), 0.0001);
    assertEquals(21, result.getInt8(1));
    assertEquals("다", result.getText(2));
    assertEquals("1970-01-01 00:15:00.019", result.getTimeDate(3).toString());
  }

  @Test
  public void testUnicodeConvert() {
    TextDatum datum = new TextDatum("가가가가");
    BigDecimal decimal = HistogramUtil.unicodeCharsToBigDecimal(datum.asUnicodeChars(), 8, true);
    String result = new String(HistogramUtil.bigDecimalToUnicodeChars(decimal));
    assertEquals(datum.asChars(), result.trim());
  }

  @Test
  public void testUnicodeConvert2() {
    TextDatum datum = new TextDatum("가가가가");
    BigDecimal decimal = HistogramUtil.unicodeCharsToBigDecimal(datum.asUnicodeChars(), 8, false);
    TextDatum result = DatumFactory.createText(Convert.chars2utf(HistogramUtil.bigDecimalToUnicodeChars(decimal)));
    assertEquals(datum.asChars(), result.asChars().trim());
  }

  @Test
  public void testUnicodeConvert3() {
    TextDatum datum = new TextDatum("가가");
    BigDecimal decimal = HistogramUtil.unicodeCharsToBigDecimal(datum.asUnicodeChars(), 6, true);
    decimal = decimal.multiply(BigDecimal.valueOf(2));
    TextDatum result = DatumFactory.createText(Convert.chars2utf(HistogramUtil.bigDecimalToUnicodeChars(decimal)));

    Tuple tuple = getVTuple(datum);
    AnalyzedSortSpec[] sortSpecs = new AnalyzedSortSpec[1];
    sortSpecs[0] = new AnalyzedSortSpec(new SortSpec(new Column("col", Type.TEXT), true, false));
    sortSpecs[0].setHasNullValue(false);
    sortSpecs[0].setPureAscii(false);
    sortSpecs[0].setMaxLength(6);
    sortSpecs[0].setMinValue(DatumFactory.createText("가"));
    sortSpecs[0].setMaxValue(DatumFactory.createText("하하하하하하"));
    tuple = HistogramUtil.incrementVector(sortSpecs, tuple, tuple, 1);
    assertEquals(result, tuple.asDatum(0));
  }

  @Test
  public void testNormalizeTuple() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple1 = getVTuple(NullDatum.get(), DatumFactory.createInt8(10),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    Tuple tuple2 = getVTuple(DatumFactory.createFloat8(0.5), NullDatum.get(),
        DatumFactory.createText("나"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    assertTrue(comparator.compare(tuple1, tuple2) > 0);
    BigDecimal[] n1 = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, tuple1);
    BigDecimal[] n2 = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, tuple2);
    assertTrue(HistogramUtil.compareNormTuples(n1, n2) > 0);

    Tuple denorm = HistogramUtil.denormalizeAsValue(analyzedSpecs, n1);
    assertEquals(tuple1, denorm);
    denorm = HistogramUtil.denormalizeAsValue(analyzedSpecs, n2);
    assertEquals(tuple2, denorm);
  }

  @Test(expected = ArithmeticException.class)
  public void testOverflow() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple1 = getVTuple(DatumFactory.createFloat8(1000), DatumFactory.createInt8(10),
        DatumFactory.createText("가가가가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    HistogramUtil.normalizeTupleAsValue(analyzedSpecs, tuple1);
  }

  @Test(expected = ArithmeticException.class)
  public void testUnderflow() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple1 = getVTuple(DatumFactory.createFloat8(0), DatumFactory.createInt8(-10),
        DatumFactory.createText("가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    HistogramUtil.normalizeTupleAsValue(analyzedSpecs, tuple1);
  }

  @Test
  public void testIncrementNormTuples() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    BigDecimal[] norm = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, tuple);
    BigDecimal[] interval = HistogramUtil.normalizeTupleAsVector(analyzedSpecs, totalBase);

    double amount = 10;
    BigDecimal[] incremented = HistogramUtil.increment(analyzedSpecs, norm, interval, amount);
    Tuple actual = HistogramUtil.denormalizeAsValue(analyzedSpecs, incremented);

    Tuple expected = getVTuple(
        DatumFactory.createFloat8(0.1 + totalBase.getFloat8(0) * amount),
        DatumFactory.createInt8(10 + totalBase.getInt8(1) * (long)(amount + 1)),
        DatumFactory.createText(Convert.chars2utf(
            HistogramUtil.bigDecimalToUnicodeChars(
                HistogramUtil.unicodeCharsToBigDecimal(tuple.getUnicodeChars(2), analyzedSpecs[2].getMaxLength(), false)
                    .add(
                        BigDecimal.valueOf(amount).multiply(HistogramUtil.unicodeCharsToBigDecimal(totalBase.getUnicodeChars(2), analyzedSpecs[2].getMaxLength(), true))
                    ).subtract(analyzedSpecs[2].getMax()).add(analyzedSpecs[2].getMin())))
        ),
        DatumFactory.createTimestampDatumWithJavaMillis(1000 + ((TimestampDatum)totalBase.asDatum(3)).getJavaTimestamp() * (long)amount)
    );
    assertEquals(expected, actual);
  }

  @Test
  public void testIncrementNormTuples2() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    BigDecimal[] norm = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, tuple);
    BigDecimal[] interval = HistogramUtil.normalizeTupleAsVector(analyzedSpecs, totalBase);
    BigDecimal[] incremented = HistogramUtil.increment(analyzedSpecs, norm, interval, 1);
    Tuple denorm = HistogramUtil.denormalizeAsValue(analyzedSpecs, incremented);
    Tuple expected = getVTuple(
        DatumFactory.createFloat8(0.1 + totalBase.getFloat8(0)),
        DatumFactory.createInt8(10 + totalBase.getInt8(1)),
        DatumFactory.createText(Convert.chars2utf(
            HistogramUtil.bigDecimalToUnicodeChars(
                HistogramUtil.unicodeCharsToBigDecimal(tuple.getUnicodeChars(2), analyzedSpecs[2].getMaxLength(), false)
                    .add(HistogramUtil.unicodeCharsToBigDecimal(new char[] {1176}, analyzedSpecs[2].getMaxLength(), true))))
        ),
        DatumFactory.createTimestampDatumWithJavaMillis(1000 + ((TimestampDatum)totalBase.asDatum(3)).getJavaTimestamp())
    );
    assertEquals(expected, denorm);
  }

  @Test
  public void testIncrementNormTuple3() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple start = getVTuple(DatumFactory.createFloat8(960.6), DatumFactory.createInt8(133),
        DatumFactory.createText("둝"), DatumFactory.createTimestamp("1970-01-01 00:15:00.019"));
    BigDecimal[] normTuple = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, start);
    BigDecimal[] normInter = HistogramUtil.normalizeTupleAsVector(analyzedSpecs, totalBase);
    BigDecimal[] incremented, decremented;
    for (int i = 0; i < 200; i++) {
      incremented = HistogramUtil.increment(analyzedSpecs, normTuple, normInter, 1);
      assertTrue(HistogramUtil.compareNormTuples(normTuple, incremented) < 0);
      decremented = HistogramUtil.increment(analyzedSpecs, incremented, normInter, -1);
      assertEquals(normTuple[0].doubleValue(), decremented[0].doubleValue(), 0.00000001);
      assertEquals(normTuple[1].doubleValue(), decremented[1].doubleValue(), 0.00000001);
      assertEquals(normTuple[2].doubleValue(), decremented[2].doubleValue(), 0.00000001);
      assertEquals(normTuple[3].doubleValue(), decremented[3].doubleValue(), 0.00000001);
      normTuple = incremented;
    }
  }

  @Test
  public void testIncrementTuple() {
    //    totalBase = getVTuple(DatumFactory.createFloat8(0.5), DatumFactory.createInt8(10),
//        DatumFactory.createText("가가가"),
//        DatumFactory.createTimestampDatumWithJavaMillis(1000));

    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple start = getVTuple(DatumFactory.createFloat8(960.6), DatumFactory.createInt8(133),
        DatumFactory.createText("둝"), DatumFactory.createTimestamp("1970-01-01 00:15:00.019"));
    Tuple incremented = HistogramUtil.incrementValue(analyzedSpecs, start, totalBase, 200);
    Tuple expected = getVTuple(DatumFactory.createFloat8(1061.6), DatumFactory.createInt8(355),
        DatumFactory.createText("숏"), DatumFactory.createTimestamp("1970-01-01 00:01:40.028"));
    assertEquals(expected, incremented);

    Tuple decremented = HistogramUtil.incrementValue(analyzedSpecs, incremented, totalBase, -200);
    assertEquals(start, decremented);
  }

  @Test
  public void testIncrementTuple2() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple start = getVTuple(DatumFactory.createFloat8(960.6), DatumFactory.createInt8(133),
        DatumFactory.createText("둝"), DatumFactory.createTimestamp("1970-01-01 00:15:00.019"));
    Tuple incremented = HistogramUtil.incrementValue(analyzedSpecs, start, totalBase, 0.1);
    Tuple expected = getVTuple(DatumFactory.createFloat8(960.65), DatumFactory.createInt8(134),
        DatumFactory.createText("듓"), DatumFactory.createTimestamp("1970-01-01 00:15:00.119"));
    assertEquals(expected, incremented);

    Tuple decremented = HistogramUtil.incrementValue(analyzedSpecs, incremented, totalBase, -0.1);
    assertEquals(start, decremented);
  }

//  @Test
//  public void testIncrementTuple3() {
//    prepareHistogram(TRUE_SET, FALSE_SET);
//    Tuple start = getVTuple(DatumFactory.createFloat8(959.6), DatumFactory.createInt8(12),
//        DatumFactory.createText("쑈쑈쥄烀烀毄"), DatumFactory.createTimestamp("1970-01-01 00:15:00.01"));
//    Tuple incremented, decremented;
//    for (int i = 0; i < 200; i++) {
//      incremented = HistogramUtil.increment(analyzedSpecs, start, totalBase, 1);
//      assertTrue(comparator.compare(start, incremented) < 0);
//      decremented = HistogramUtil.increment(analyzedSpecs, incremented, totalBase, -1);
//      assertEquals(i + " th tuples are different", start, decremented);
//      start = incremented;
//    }
//  }

  @Test
  public void testSplitBucket() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple start = getVTuple(DatumFactory.createFloat8(959.6), DatumFactory.createInt8(12),
        DatumFactory.createText("쑈쑈쥄烀烀毄"), DatumFactory.createTimestamp("1970-01-01 00:15:00.01"));
    Tuple end = getVTuple(DatumFactory.createFloat8(1060.6), DatumFactory.createInt8(22),
        DatumFactory.createText("ࠐࠐඒ꡴꡴ꋲ"), DatumFactory.createTimestamp("1970-01-01 00:01:40.01"));
    Bucket bucket = histogram.getBucket(start, end);
    List<Bucket> buckets = HistogramUtil.splitBucket(histogram, analyzedSpecs, bucket, 1);
    assertEquals(200, buckets.size());
    for (Bucket eachBucket : buckets) {
      assertTrue(eachBucket.getCount() > 0);
    }
  }

  @Test
  public void testSplitBucket2() {
//    prepareHistogram(TRUE_SET, FALSE_SET);
//    Tuple start = getVTuple(DatumFactory.createFloat8(959.6), DatumFactory.createInt8(12),
//        DatumFactory.createText("쑈쑈쥄烀烀毄"), DatumFactory.createTimestamp("1970-01-01 00:15:00.01"));
//    Tuple end = getVTuple(DatumFactory.createFloat8(1060.6), DatumFactory.createInt8(22),
//        DatumFactory.createText("ࠐࠐඒ꡴꡴ꋲ"), DatumFactory.createTimestamp("1970-01-01 00:01:40.01"));
//    Bucket bucket = histogram.getBucket(start, end);
//    Tuple interval = getVTuple(DatumFactory.createFloat8(40.d), DatumFactory.createInt8(500),
//        DatumFactory.createText("aa"), DatumFactory.createTimestampDatumWithJavaMillis(100000));
//    List<Bucket> buckets = HistogramUtil.splitBucket(histogram, analyzedSpecs, bucket, interval);
//    assertEquals(2, buckets.size());
//    for (Bucket eachBucket : buckets) {
//      assertTrue(eachBucket.getCount() > 0);
//    }
  }

  @Test
  public void testWeightedSum() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    BigDecimal[] normTuple = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, tuple);
    int[] scales = new int[] {normTuple[0].scale(), normTuple[1].scale(), normTuple[2].scale(), normTuple[3].scale()};
    BigDecimal sum = HistogramUtil.weightedSum(normTuple, scales);
    BigDecimal[] fromSum = HistogramUtil.normTupleFromWeightedSum(analyzedSpecs, sum, scales);

    assertEquals(normTuple[0].doubleValue(), fromSum[0].doubleValue(), 0.00000001);
    assertEquals(normTuple[1].doubleValue(), fromSum[1].doubleValue(), 0.00000001);
    assertEquals(normTuple[2].doubleValue(), fromSum[2].doubleValue(), 0.00000001);
    assertEquals(normTuple[3].doubleValue(), fromSum[3].doubleValue(), 0.00000001);
  }

  @Test
  public void testWeightedSum2() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Bucket bucket = histogram.getSortedBuckets().get(0);
    Tuple start = bucket.getStartKey();
    Tuple end = bucket.getEndKey();
    BigDecimal[] normStart = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, start);
    BigDecimal[] normEnd = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, end);
    int[] maxScales = HistogramUtil.maxScales(normEnd, normStart);
    BigDecimal startVal = HistogramUtil.weightedSum(normStart, maxScales);
    BigDecimal endVal = HistogramUtil.weightedSum(normEnd, maxScales);
    BigDecimal interVal = endVal.subtract(startVal).divide(BigDecimal.valueOf(5), 128, BigDecimal.ROUND_HALF_UP);
    BigDecimal[] normInter = HistogramUtil.normTupleFromWeightedSum(analyzedSpecs, interVal, maxScales);
    Tuple interval = HistogramUtil.denormalizeAsVector(analyzedSpecs, normInter);
    // TODO
  }

  @Test
  public void testRefineToEquiDepth() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    BigDecimal avgCount = totalCount.divide(BigDecimal.valueOf(21), MathContext.DECIMAL128);
    HistogramUtil.refineToEquiDepth(histogram, avgCount, analyzedSpecs);
    List<Bucket> buckets = histogram.getSortedBuckets();
    assertEquals(21, buckets.size());
    Tuple prevEnd = null;
    long count = 0;
    for (Bucket bucket : buckets) {
      if (prevEnd != null) {
        assertEquals(prevEnd, bucket.getStartKey());
      }
      prevEnd = bucket.getEndKey();
      count += bucket.getCount();
    }
    assertEquals(totalCount.longValue(), count);
  }

  @Test
  public void testDiff() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    Tuple result = HistogramUtil.incrementValue(analyzedSpecs, tuple, totalBase, 1);

    long diff = HistogramUtil.diff(analyzedSpecs, totalBase, tuple, result);
    assertEquals(1, diff);

    diff = HistogramUtil.diff(analyzedSpecs, totalBase, result, tuple);
    assertEquals(1, diff);
  }

  @Test
  public void testDiffTuple() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    Tuple result = HistogramUtil.incrementValue(analyzedSpecs, tuple, totalBase, 1);

    Tuple diff = HistogramUtil.diff(analyzedSpecs, tuple, result);
    assertEquals(totalBase, diff);
  }

  @Test
  public void testDiffTuple2() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    Tuple result = HistogramUtil.incrementValue(analyzedSpecs, tuple, totalBase, 10);

    Tuple diff = HistogramUtil.diff(analyzedSpecs, tuple, result);
    Tuple expected = getVTuple(DatumFactory.createFloat8(5.0), DatumFactory.createInt8(110),
        DatumFactory.createText(Convert.chars2utf(
            HistogramUtil.bigDecimalToUnicodeChars(
                HistogramUtil.unicodeCharsToBigDecimal("가".toCharArray()).add(BigDecimal.valueOf(11760))
                    .subtract(analyzedSpecs[2].getMax()))
        )),
        DatumFactory.createTimestamp("1970-01-01 00:00:10"));

    assertEquals(expected, diff);
  }

  @Test
  public void testDiffTuple3() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple start = getVTuple(DatumFactory.createFloat8(960.6), DatumFactory.createInt8(133),
        DatumFactory.createText("둝"), DatumFactory.createTimestamp("1970-01-01 00:15:00.019"));
//    Tuple end = getVTuple(DatumFactory.createFloat8(1061.6), DatumFactory.createInt8(355),
//        DatumFactory.createText("숏"), DatumFactory.createTimestamp("1970-01-01 00:01:40.028"));
    double amount = 79;
    Tuple end = HistogramUtil.incrementValue(analyzedSpecs, start, totalBase, amount);

    Tuple diff = HistogramUtil.diff(analyzedSpecs, start, end);
    Tuple intervalSum = HistogramUtil.incrementVector(analyzedSpecs, totalBase, totalBase, amount-1);

    System.out.println(HistogramUtil.unicodeCharsToBigDecimal("च".toCharArray()));
    System.out.println(HistogramUtil.unicodeCharsToBigDecimal("Օ".toCharArray()));

//    assertEquals(end, HistogramUtil.increment(analyzedSpecs, start, totalBase, 200));
//    assertEquals(end, HistogramUtil.increment(analyzedSpecs, start, expected, 1));
    assertEquals(intervalSum, diff);
  }

  @Test
  public void testDiffTuple4() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(10));
    Tuple expected = getVTuple(DatumFactory.createFloat8(0.6), DatumFactory.createInt8(12),
        DatumFactory.createText(Convert.chars2utf(HistogramUtil.bigDecimalToUnicodeChars(
            HistogramUtil.unicodeCharsToBigDecimal("가".toCharArray(), 1, false)
            .add(HistogramUtil.unicodeCharsToBigDecimal(new char[] {1176}, 1, true))
        ))),
        DatumFactory.createTimestampDatumWithJavaMillis(1010));

    Tuple result = HistogramUtil.incrementValue(analyzedSpecs, tuple, totalBase, 1);

    Tuple diff = HistogramUtil.diff(analyzedSpecs, tuple, result);

    BigDecimal[] n1 = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, tuple);
    BigDecimal[] e1 = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, result);
    BigDecimal[] normDiff = HistogramUtil.normalizeTupleAsVector(analyzedSpecs, diff);
    BigDecimal[] expDiff = HistogramUtil.normalizeTupleAsVector(analyzedSpecs, totalBase);

    int[] maxScales = HistogramUtil.maxScales(n1, e1, normDiff, expDiff);

    BigDecimal sum1 = HistogramUtil.weightedSum(n1, maxScales);
    BigDecimal sum2 = HistogramUtil.weightedSum(e1, maxScales);
    BigDecimal sumDiffVal = sum2.subtract(sum1);

    BigDecimal normDiffVal = HistogramUtil.weightedSum(normDiff, maxScales);
    BigDecimal expDiffVal = HistogramUtil.weightedSum(expDiff, maxScales);

//    System.out.println("end - start: " + sumDiffVal);
//    System.out.println("diff       : " + normDiffVal);
//    System.out.println("expected   : " + expDiffVal);

    assertEquals(sumDiffVal, normDiffVal);
    assertEquals(normDiffVal, expDiffVal);
    assertEquals(expected, result);
  }

  @Test
  public void testDiffTuple5() {
    //    totalBase = getVTuple(DatumFactory.createFloat8(0.5), DatumFactory.createInt8(10),
//        DatumFactory.createText("가가가"),
//        DatumFactory.createTimestampDatumWithJavaMillis(1000));

//    columnStatsList.get(0).setMaxValue(DatumFactory.createFloat8(10000000d));
//    columnStatsList.get(1).setMaxValue(DatumFactory.createInt8(1000));
//    columnStatsList.get(2).setMaxValue(DatumFactory.createText("하하하하하하"));
//    columnStatsList.get(3).setMaxValue(DatumFactory.createTimestampDatumWithJavaMillis(1000000));

    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple start = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2),
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(10));
    Tuple expected = getVTuple(DatumFactory.createFloat8(5050.1), DatumFactory.createInt8(62),
        DatumFactory.createText(Convert.chars2utf(HistogramUtil.bigDecimalToUnicodeChars(
            HistogramUtil.unicodeCharsToBigDecimal("가".toCharArray(), 1, false)
                .add(
                    HistogramUtil.unicodeCharsToBigDecimal(new char[] {1176}, 1, true)
                        .multiply(BigDecimal.valueOf(10010))
                ).subtract(analyzedSpecs[2].getMax().multiply(BigDecimal.valueOf(6)))
        ))),
        DatumFactory.createTimestampDatumWithJavaMillis(10));

    Tuple incremented = HistogramUtil.incrementValue(analyzedSpecs, start, totalBase, 10000);
    assertEquals(expected, incremented);

    expected = getVTuple(DatumFactory.createFloat8(5050), DatumFactory.createInt8(60),
        DatumFactory.createText(Convert.chars2utf(HistogramUtil.bigDecimalToUnicodeChars(
            HistogramUtil.unicodeCharsToBigDecimal("가가가".toCharArray(), 6, true)
                .multiply(BigDecimal.valueOf(10010))
                .subtract(analyzedSpecs[2].getMax().multiply(BigDecimal.valueOf(6)))
        ))),
        DatumFactory.createTimestampDatumWithJavaMillis(0));

    Tuple diff = HistogramUtil.diff(analyzedSpecs, start, incremented);
    Tuple incrementedBase = HistogramUtil.incrementVector(analyzedSpecs, totalBase, totalBase, 9999);

    assertEquals(expected, diff);
    assertEquals(diff, incrementedBase);

    BigDecimal[] n1 = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, start);
    BigDecimal[] e1 = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, incremented);
    BigDecimal[] normDiff = HistogramUtil.normalizeTupleAsVector(analyzedSpecs, diff);
    BigDecimal[] expDiff = HistogramUtil.normalizeTupleAsVector(analyzedSpecs, incrementedBase);

    int[] maxScales = HistogramUtil.maxScales(n1, e1, normDiff, expDiff);

    BigDecimal sum1 = HistogramUtil.weightedSum(n1, maxScales);
    BigDecimal sum2 = HistogramUtil.weightedSum(e1, maxScales);
    BigDecimal sumDiffVal = sum2.subtract(sum1);

    BigDecimal normDiffVal = HistogramUtil.weightedSum(normDiff, maxScales);
    BigDecimal expDiffVal = HistogramUtil.weightedSum(expDiff, maxScales);

    System.out.println("end - start: " + sumDiffVal);
    System.out.println("diff       : " + normDiffVal);
    System.out.println("expected   : " + expDiffVal);

    assertEquals(sumDiffVal, normDiffVal);
    assertEquals(normDiffVal, expDiffVal);
  }

//  @Test
//  public void testMeanInterval() {
//    prepareHistogram(TRUE_SET, FALSE_SET);
//    Tuple inter1 = totalBase;
//    Tuple inter2 = HistogramUtil.incrementVector(analyzedSpecs, inter1, totalBase, 2);
//    Tuple mean = HistogramUtil.getMeanInterval(analyzedSpecs, inter1, inter2);
//    Tuple expected = HistogramUtil.incrementVector(analyzedSpecs, inter1, totalBase, 1);
//    assertEquals(expected, mean);
//  }
//
//  @Test
//  public void testMeanInterval2() {
//    prepareHistogram(TRUE_SET, FALSE_SET);
//    Tuple inter1 = totalBase;
//    Tuple inter2 = getVTuple(DatumFactory.createFloat8(15.5), DatumFactory.createInt8(3),
//        DatumFactory.createText("나나나"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
//    Tuple mean = HistogramUtil.getMeanInterval(analyzedSpecs, inter1, inter2);
//    assertEquals(7.5, mean.getFloat8(0), 0.000001);
//    assertEquals(507, mean.getInt8(1));
//    assertEquals("까까까", mean.getText(2));
//    assertEquals("1970-01-01 00:00:01", mean.getTimeDate(3).toString());
//  }

  @Test
  public void testGetSubBucket() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Bucket bucket = histogram.getSortedBuckets().get(0);
    Tuple start = HistogramUtil.incrementValue(analyzedSpecs, bucket.getStartKey(), totalBase, 2);
    Tuple end = HistogramUtil.incrementValue(analyzedSpecs, start, totalBase, 3);
    Bucket subBucket = HistogramUtil.getSubBucket(histogram, analyzedSpecs, bucket, start, end);
    assertEquals(start, subBucket.getStartKey());
    assertEquals(end, subBucket.getEndKey());
    assertEquals(3, subBucket.getCount(), 0.0000001);
  }
}
