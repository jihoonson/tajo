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
        DatumFactory.createText("d"),
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
        DatumFactory.createText("가가가"), DatumFactory.createTimestampDatumWithJavaMillis(10));
    Datum[] minDatums = new Datum[schema.size()];
    for (int i = 0; i < minDatums.length; i++) {
      minDatums[i] = start.asDatum(i);
    }
    Tuple end;
    if (nullFirst.cardinality() > 0) {
      Tuple tuple = getNullIncludeVTuple(nullFirst, start, false);
      histogram.updateBucket(tuple, start, totalBase, 50);
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
    columnStatsList.get(2).setMaxValue(DatumFactory.createText("하하하하하하"));
    columnStatsList.get(3).setMaxValue(DatumFactory.createTimestampDatumWithJavaMillis(1000000));

    analyzedSpecs = HistogramUtil.toAnalyzedSortSpecs(sortSpecs, columnStatsList);
    analyzedSpecs[2].setPureAscii(false);
    analyzedSpecs[2].setMaxLength(6);

    for (int i = 0; i < 20; i++) {
      long count = (i + 1) * 10;
      end = HistogramUtil.increment(analyzedSpecs, start, totalBase, count);
      histogram.updateBucket(start, end, totalBase, count);
      start = end;
      card += count;
    }
    if (nullFirst.cardinality() < schema.size()) {
      nullFirst.flip(0, schema.size());
      Tuple tuple = getNullIncludeVTuple(nullFirst, start, true);
      histogram.updateBucket(start, tuple, totalBase, 50);
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
        DatumFactory.createText("가가가"), DatumFactory.createTimestampDatumWithJavaMillis(10));
    Tuple result = HistogramUtil.increment(analyzedSpecs, tuple, totalBase, 10);

    assertEquals(5.1, result.getFloat8(0), 0.0001);
    assertEquals(102, result.getInt8(1));
    assertEquals("굀굀굀ŠŠЈ", result.getText(2));
    assertEquals("1970-01-01 00:00:10.01", result.getTimeDate(3).toString());

    result = HistogramUtil.increment(analyzedSpecs, result, totalBase, -10);

    assertEquals(0.1, result.getFloat8(0), 0.0001);
    assertEquals(2, result.getInt8(1));
    assertEquals("가가가   ", result.getText(2));
    assertEquals("1970-01-01 00:00:00.01", result.getTimeDate(3).toString());
  }

  @Test
  public void testIncrement2() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(969.1), DatumFactory.createInt8(21),
        DatumFactory.createText("ӽ撷가각"), DatumFactory.createTimestamp("1970-01-01 00:15:00.019"));
    Tuple incremented = HistogramUtil.increment(analyzedSpecs, tuple, totalBase, 3);

    assertEquals(970.6, incremented.getFloat8(0), 0.000001);
    assertEquals(51, incremented.getInt8(1));
    assertEquals("՝攗걠걡\u0080Ō", incremented.getText(2));
    assertEquals("1970-01-01 00:15:03.019", incremented.getTimeDate(3).toString());

    Tuple result = HistogramUtil.increment(analyzedSpecs, incremented, totalBase, -3);

    assertEquals(969.1, result.getFloat8(0), 0.0001);
    assertEquals(21, result.getInt8(1));
    assertEquals("ӽ撷가각  ", result.getText(2));
    assertEquals("1970-01-01 00:15:00.019", result.getTimeDate(3).toString());
  }

  @Test
  public void testUnicodeConvert() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    TextDatum datum = new TextDatum("가가가가");
    BigDecimal decimal = HistogramUtil.unicodeCharsToBigDecimal(datum.asUnicodeChars(), 8, true);
    String result = new String(HistogramUtil.bigDecimalToUnicodeChars(decimal));
    assertEquals(datum.asChars().trim(), result.trim());
  }

  @Test
  public void testUnicodeConvert2() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    TextDatum datum = new TextDatum("가가가가");
    BigDecimal decimal = HistogramUtil.unicodeCharsToBigDecimal(datum.asUnicodeChars(), 8, false);
    TextDatum result = DatumFactory.createText(Convert.chars2utf(HistogramUtil.bigDecimalToUnicodeChars(decimal)));
    assertEquals(datum.asChars().trim(), result.asChars().trim());
  }

  @Test
  public void testNormalizeTuple() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple1 = getVTuple(NullDatum.get(), DatumFactory.createInt8(10),
        DatumFactory.createText("가가가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    Tuple tuple2 = getVTuple(DatumFactory.createFloat8(0.5), NullDatum.get(),
        DatumFactory.createText("나나나나나나"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    assertTrue(comparator.compare(tuple1, tuple2) > 0);
    BigDecimal[] n1 = HistogramUtil.normalize(analyzedSpecs, tuple1, false, true);
    BigDecimal[] n2 = HistogramUtil.normalize(analyzedSpecs, tuple2, false, true);
    assertTrue(HistogramUtil.compareNormTuples(n1, n2) > 0);

    Tuple denorm = HistogramUtil.denormalize(analyzedSpecs, n1, true);
    assertEquals(tuple1, denorm);
    denorm = HistogramUtil.denormalize(analyzedSpecs, n2, true);
    assertEquals(tuple2, denorm);
  }

  @Test(expected = ArithmeticException.class)
  public void testOverflow() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple1 = getVTuple(DatumFactory.createFloat8(1000), DatumFactory.createInt8(10),
        DatumFactory.createText("가가가가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    HistogramUtil.normalize(analyzedSpecs, tuple1, false, true);
  }

  @Test(expected = ArithmeticException.class)
  public void testUnderflow() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple1 = getVTuple(DatumFactory.createFloat8(0), DatumFactory.createInt8(-10),
        DatumFactory.createText("가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    HistogramUtil.normalize(analyzedSpecs, tuple1, false, true);
  }

  @Test
  public void testIncrementNormTuples() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가가가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    BigDecimal[] norm = HistogramUtil.normalize(analyzedSpecs, tuple, false, true);
    BigDecimal[] interval = HistogramUtil.normalize(analyzedSpecs, totalBase, true, false);
    BigDecimal[] incremented = HistogramUtil.increment(norm, interval, 10);
    Tuple denorm = HistogramUtil.denormalize(analyzedSpecs, incremented, true);
    Tuple expected = getVTuple(
        DatumFactory.createFloat8(0.1 + totalBase.getFloat8(0) * 10),
        DatumFactory.createInt8(10 + totalBase.getInt8(1) * 10),
        DatumFactory.createText(Convert.chars2utf(
            HistogramUtil.bigDecimalToUnicodeChars(
                HistogramUtil.unicodeCharsToBigDecimal(tuple.getUnicodeChars(2), analyzedSpecs[2].getMaxLength(), false)
                    .add(HistogramUtil.unicodeCharsToBigDecimal(new char[] {'d'}, analyzedSpecs[2].getMaxLength(), true).multiply(BigDecimal.TEN))))
        ),
        DatumFactory.createTimestampDatumWithJavaMillis(1000 + ((TimestampDatum)totalBase.asDatum(3)).getJavaTimestamp() * 10)
    );
    assertEquals(expected, denorm);
  }

  @Test
  public void testIncrementNormTuples2() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가가가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    BigDecimal[] norm = HistogramUtil.normalize(analyzedSpecs, tuple, false, true);
    BigDecimal[] interval = HistogramUtil.normalize(analyzedSpecs, totalBase, true, false);
    BigDecimal[] incremented = HistogramUtil.increment(norm, interval, 1);
    Tuple denorm = HistogramUtil.denormalize(analyzedSpecs, incremented, true);
    Tuple expected = getVTuple(
        DatumFactory.createFloat8(0.1 + totalBase.getFloat8(0)),
        DatumFactory.createInt8(10 + totalBase.getInt8(1)),
        DatumFactory.createText(Convert.chars2utf(
            HistogramUtil.bigDecimalToUnicodeChars(
                HistogramUtil.unicodeCharsToBigDecimal(tuple.getUnicodeChars(2), analyzedSpecs[2].getMaxLength(), false)
                    .add(HistogramUtil.unicodeCharsToBigDecimal(new char[] {'d'}, analyzedSpecs[2].getMaxLength(), true))))
        ),
        DatumFactory.createTimestampDatumWithJavaMillis(1000 + ((TimestampDatum)totalBase.asDatum(3)).getJavaTimestamp())
    );
    assertEquals(expected, denorm);
  }

  @Test
  public void testSplitBucket() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple start = getVTuple(DatumFactory.createFloat8(950.1), DatumFactory.createInt8(2),
        DatumFactory.createText("쐨쐨쐨ᡈᡊ㙨"), DatumFactory.createTimestamp("1970-01-01 00:15:00.01"));
    Tuple end = getVTuple(DatumFactory.createFloat8(1050.1), DatumFactory.createInt8(2),
        DatumFactory.createText("ߐߐߏ寮寰꼯"), DatumFactory.createTimestamp("1970-01-01 00:01:40.01"));
    Bucket bucket = histogram.getBucket(start, end, totalBase);
    List<Bucket> buckets = HistogramUtil.splitBucket(histogram, analyzedSpecs, bucket, totalBase);
    assertEquals(199, buckets.size());
    for (Bucket eachBucket : buckets) {
      assertTrue(eachBucket.getCount() > 0);
    }
  }

  @Test
  public void testSplitBucket2() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple start = getVTuple(DatumFactory.createFloat8(950.1), DatumFactory.createInt8(2),
        DatumFactory.createText("쐨쐨쐨ᡈᡊ㙨"), DatumFactory.createTimestamp("1970-01-01 00:15:00.01"));
    Tuple end = getVTuple(DatumFactory.createFloat8(1050.1), DatumFactory.createInt8(2),
        DatumFactory.createText("ߐߐߏ寮寰꼯"), DatumFactory.createTimestamp("1970-01-01 00:01:40.01"));
    Bucket bucket = histogram.getBucket(start, end, totalBase);
    Tuple interval = getVTuple(DatumFactory.createFloat8(40.d), DatumFactory.createInt8(500),
        DatumFactory.createText("aa"), DatumFactory.createTimestampDatumWithJavaMillis(100000));
    List<Bucket> buckets = HistogramUtil.splitBucket(histogram, analyzedSpecs, bucket, interval);
    assertEquals(3, buckets.size());
    for (Bucket eachBucket : buckets) {
      assertTrue(eachBucket.getCount() > 0);
    }
  }

//  @Test
  public void testWeightedSumOfNormTuple() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가가가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    BigDecimal[] normTuple = HistogramUtil.normalizeTupleAsValue(analyzedSpecs, tuple);
    BigDecimal sum = HistogramUtil.weightedSum(analyzedSpecs, normTuple);
    BigDecimal[] fromSum = HistogramUtil.normTupleFromWeightedSum(analyzedSpecs, sum, HistogramUtil.getMaxScale(normTuple),
        new int[] {normTuple[0].scale(), normTuple[1].scale(), normTuple[2].scale(), normTuple[3].scale()});

    assertEquals(normTuple[0].doubleValue(), fromSum[0].doubleValue(), 0.00000001);
    assertEquals(normTuple[1].doubleValue(), fromSum[1].doubleValue(), 0.00000001);
    assertEquals(normTuple[2].doubleValue(), fromSum[2].doubleValue(), 0.00000001);
    assertEquals(normTuple[3].doubleValue(), fromSum[3].doubleValue(), 0.00000001);
  }

  @Test
  public void testRefineToEquiDepth() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    BigDecimal avgCount = totalCount.divide(BigDecimal.valueOf(21), MathContext.DECIMAL128);
    HistogramUtil.refineToEquiDepth(histogram, avgCount, analyzedSpecs);
    List<Bucket> buckets = histogram.getSortedBuckets();
    assertEquals(21, buckets.size());
    Tuple prevEnd = null;
    for (Bucket bucket : buckets) {
      if (prevEnd != null) {
        assertEquals(prevEnd, bucket.getStartKey());
      }
      prevEnd = bucket.getEndKey();
      assertTrue(bucket.getCount() == avgCount.longValue() || bucket.getCount() == avgCount.longValue() + 1);
    }
  }

  @Test
  public void testDiff() {
    prepareHistogram(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가가가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    Tuple result = HistogramUtil.increment(analyzedSpecs, tuple, totalBase, 1);

    Tuple diff = HistogramUtil.diff(analyzedSpecs, tuple, result);
    assertEquals(0.5, diff.getFloat8(0), 0.0001);
    assertEquals(10, diff.getInt8(1));
    assertEquals("     d", diff.getText(2));
    assertEquals("1970-01-01 00:00:01", diff.getTimeDate(3).toString());

    diff = HistogramUtil.diff(analyzedSpecs, result, tuple);
    assertEquals(0.5, diff.getFloat8(0), 0.0001);
    assertEquals(10, diff.getInt8(1));
    assertEquals("     d", diff.getText(2));
    assertEquals("1970-01-01 00:00:01", diff.getTimeDate(3).toString());
  }

  @Test
  public void testDiff2() {
    prepareHistogram(FALSE_SET, TRUE_SET);

    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(10),
        DatumFactory.createText("가가가가가가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));
    Tuple result = HistogramUtil.increment(analyzedSpecs, tuple, totalBase, 1);

    Tuple diff = HistogramUtil.diff(analyzedSpecs, tuple, result);
    assertEquals(0.5, diff.getFloat8(0), 0.0001);
    assertEquals(10, diff.getInt8(1));
    assertEquals("     d", diff.getText(2));
    assertEquals("1970-01-01 00:00:01", diff.getTimeDate(3).toString());

    diff = HistogramUtil.diff(analyzedSpecs, result, tuple);
    assertEquals(0.5, diff.getFloat8(0), 0.0001);
    assertEquals(10, diff.getInt8(1));
    assertEquals("     d", diff.getText(2));
    assertEquals("1970-01-01 00:00:01", diff.getTimeDate(3).toString());
  }
}
