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
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
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
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHistogramUtil {

  private static Schema schema;
  private static SortSpec[] sortSpecs;
  private static FreqHistogram histogram;
  private static Tuple totalBase;
  private static List<ColumnStats> columnStatsList;
  private static BigDecimal totalCount;

  private static BitSet FALSE_SET;
  private static BitSet TRUE_SET;

  private static boolean[] isPureAscii = new boolean[] {false, false, false, false};
  private static int[] maxLength = new int[] {0, 0, 3, 0};

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
        DatumFactory.createText("가"), DatumFactory.createTimestampDatumWithJavaMillis(1000));

    columnStatsList = new ArrayList<>(schema.size());
    for (Column column : schema.getAllColumns()) {
      columnStatsList.add(new ColumnStats(column));
    }
  }

  private void prepare(BitSet asc, BitSet nullFirst) {
    sortSpecs = new SortSpec[schema.size()];
    for (int i = 0; i < sortSpecs.length; i++) {
      sortSpecs[i] = new SortSpec(schema.getColumn(i), asc.get(i), nullFirst.get(i));
    }

    histogram = new FreqHistogram(schema, sortSpecs);

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

    for (int i = 0; i < 20; i++) {
      long count = (i + 1) * 10;
//      long secondValue = start.getInt8(1) + count * totalBase.getInt8(1);
//      secondValue = secondValue > 10000 ? secondValue - 10000 : secondValue;
//      normStartText = HistogramUtil.unicodeCharsToBigDecimal(start.getUnicodeChars(2));
//      long fourthValue = ((TimestampDatum)start.asDatum(3)).getJavaTimestamp()
//          + ((TimestampDatum)totalBase.asDatum(3)).getJavaTimestamp() * count;
//      fourthValue = fourthValue > 100000 ? fourthValue - 100000 : fourthValue;
//
//      end = getVTuple(DatumFactory.createFloat8(start.getFloat8(0) + count * totalBase.getFloat8(0)),
//          DatumFactory.createInt8(secondValue),
//          DatumFactory.createText(
//              Convert.chars2utf(HistogramUtil.bigDecimalToUnicodeChars(normStartText.add(normInterval.multiply(BigDecimal.valueOf(count)))))),
//          DatumFactory.createTimestampDatumWithJavaMillis(fourthValue));
      end = HistogramUtil.increment(sortSpecs, columnStatsList, start, totalBase, count, isPureAscii, maxLength);
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
              BigDecimal base = HistogramUtil.unicodeCharsToBigDecimal(minOrMax.getUnicodeChars(i));
              BigDecimal add = HistogramUtil.unicodeCharsToBigDecimal(totalBase.getUnicodeChars(i));
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
    prepare(TRUE_SET, FALSE_SET);
    FreqHistogramProto proto = histogram.getProto();
    FreqHistogram deserialized = new FreqHistogram(proto);
    assertEquals(histogram, deserialized);
  }

  @Test
  public void testIncrement() {
    prepare(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2),
        DatumFactory.createText("가가가"), DatumFactory.createTimestampDatumWithJavaMillis(10));
    Tuple result = HistogramUtil.increment(sortSpecs, columnStatsList, tuple, totalBase, 10,
        isPureAscii, maxLength);

    assertEquals(5.1, result.getFloat8(0), 0.0001);
    assertEquals(102, result.getInt8(1));
    assertEquals("\u0007搇가가", result.getText(2));
    assertEquals("1970-01-01 00:00:10.01", result.getTimeDate(3).toString());
  }

  @Test
  public void testIncrement2() {
    prepare(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(969.1), DatumFactory.createInt8(21),
        DatumFactory.createText("ӽ撷가각"), DatumFactory.createTimestamp("1970-01-01 00:15:00.019"));
    Tuple incremented = HistogramUtil.increment(sortSpecs, columnStatsList, tuple, totalBase, 3,
        isPureAscii, maxLength);
    assertEquals(970.6, incremented.getFloat8(0), 0.000001);
    assertEquals(51, incremented.getInt8(1));
    assertEquals("ӿ梹가각", incremented.getText(2));
    assertEquals("1970-01-01 00:15:03.019", incremented.getTimeDate(3).toString());
  }

  @Test
  public void testIncrement3() {
    prepare(TRUE_SET, TRUE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(969.1), DatumFactory.createInt8(21),
        DatumFactory.createText("ӽ撷가각"), DatumFactory.createTimestamp("1970-01-01 00:15:00.019"));
    Tuple incremented = HistogramUtil.increment(sortSpecs, columnStatsList, tuple, totalBase, 3,
        isPureAscii, maxLength);
    assertEquals(970.6, incremented.getFloat8(0), 0.000001);
    assertEquals(51, incremented.getInt8(1));
    assertEquals("ӿ梹가각", incremented.getText(2));
    assertEquals("1970-01-01 00:15:03.019", incremented.getTimeDate(3).toString());
  }

  @Test
  public void testDecrement() {
    prepare(TRUE_SET, FALSE_SET);
    Tuple tuple = getVTuple(DatumFactory.createFloat8(5.1), DatumFactory.createInt8(102),
        DatumFactory.createText("\u0007搇가가"), DatumFactory.createTimestamp("1970-01-01 00:00:10.01"));
    Tuple result = HistogramUtil.increment(sortSpecs, columnStatsList, tuple, totalBase, -10,
        isPureAscii, maxLength);

    assertEquals(0.1, result.getFloat8(0), 0.0001);
    assertEquals(2, result.getInt8(1));
    assertEquals("가가가", result.getText(2));
    assertEquals(10, ((TimestampDatum)result.asDatum(3)).getJavaTimestamp());
  }

  @Test
  public void testUnicodeConvert() {
    prepare(TRUE_SET, FALSE_SET);
    TextDatum datum = new TextDatum("가가가가");
    BigDecimal decimal = HistogramUtil.unicodeCharsToBigDecimal(datum.asUnicodeChars());
    String result = new String(HistogramUtil.bigDecimalToUnicodeChars(decimal));
    assertEquals(datum.asChars(), result);
  }

  @Test
  public void testUnicodeConvert2() {
    prepare(TRUE_SET, FALSE_SET);
    TextDatum datum = new TextDatum("가가가가    ");
    BigDecimal decimal = HistogramUtil.unicodeCharsToBigDecimal(datum.asUnicodeChars());
    String result = new String(HistogramUtil.bigDecimalToUnicodeChars(decimal));
    assertEquals(datum.asChars(), result);
  }

  @Test
  public void testNormalizeTuple() {
    prepare(TRUE_SET, FALSE_SET);
    Tuple tuple1 = getVTuple(NullDatum.get(), DatumFactory.createInt8(10));
    Tuple tuple2 = getVTuple(DatumFactory.createInt8(10), NullDatum.get());
    assertTrue(histogram.comparator.compare(tuple1, tuple2) > 0);
    BigDecimal n1 = HistogramUtil.normalize(tuple1, sortSpecs, columnStatsList);
    BigDecimal n2 = HistogramUtil.normalize(tuple2, sortSpecs, columnStatsList);
    assertTrue(n1.compareTo(n2) > 0);
  }

  @Test
  public void testNormalizeTuple2() {
    prepare(FALSE_SET, FALSE_SET);
    Schema schema = new Schema();
    schema.addColumn("col1", Type.TEXT);
    schema.addColumn("col2", Type.TEXT);

    SortSpec[] sortSpecs = new SortSpec[2];
    sortSpecs[0] = new SortSpec(schema.getColumn(0), false, false);
    sortSpecs[1] = new SortSpec(schema.getColumn(1));

    List<ColumnStats> columnStatsList = new ArrayList<>(2);
    ColumnStats stats = new ColumnStats(schema.getColumn(0));
    columnStatsList.add(stats);
    stats = new ColumnStats(schema.getColumn(1));
    columnStatsList.add(stats);

    columnStatsList.get(0).setMinValue(DatumFactory.createText("하하하"));
    columnStatsList.get(0).setMaxValue(DatumFactory.createText("하하하"));
    columnStatsList.get(1).setMinValue(DatumFactory.createText("가가가가가"));
    columnStatsList.get(1).setMaxValue(DatumFactory.createText("가가가가가"));

    Tuple tuple1 = getVTuple(NullDatum.get(), DatumFactory.createText("가가가가가"));
    Tuple tuple2 = getVTuple(DatumFactory.createText("하하하"), NullDatum.get());
    BigDecimal n1 = HistogramUtil.normalize(tuple1, sortSpecs, columnStatsList);
    BigDecimal n2 = HistogramUtil.normalize(tuple2, sortSpecs, columnStatsList);
    assertTrue(n1.compareTo(n2) > 0);
  }

  @Test
  public void testDenormalize() {
    Tuple tuple1 = getVTuple(NullDatum.get(), DatumFactory.createInt8(10));
    Tuple tuple2 = getVTuple(DatumFactory.createInt8(10), NullDatum.get());
    assertTrue(histogram.comparator.compare(tuple1, tuple2) > 0);
    SortSpec[] sortSpecs = new SortSpec[2];
    sortSpecs[0] = new SortSpec(schema.getColumn(0), true, false);
    sortSpecs[1] = new SortSpec(schema.getColumn(1), true, false);
    BigDecimal n1 = HistogramUtil.normalize(tuple1, sortSpecs, columnStatsList);
    BigDecimal n2 = HistogramUtil.normalize(tuple2, sortSpecs, columnStatsList);
  }

  @Test
  public void testSplitBucket() {
    Tuple start = getVTuple(DatumFactory.createFloat8(950.1), DatumFactory.createInt8(19002));
    Tuple end = getVTuple(DatumFactory.createFloat8(1050.1), DatumFactory.createInt8(21002));
    Bucket bucket = histogram.getBucket(start, end, totalBase);
    List<Bucket> buckets = HistogramUtil.splitBucket(histogram, columnStatsList, bucket, totalBase,
        isPureAscii, maxLength);
    assertEquals(199, buckets.size());
  }

  @Test
  public void testSplitBucket2() {
    Tuple start = getVTuple(DatumFactory.createFloat8(950.1), DatumFactory.createInt8(19002));
    Tuple end = getVTuple(DatumFactory.createFloat8(1050.1), DatumFactory.createInt8(21002));
    Tuple interval = getVTuple(DatumFactory.createFloat8(40.d), DatumFactory.createInt8(500));
    Bucket bucket = histogram.getBucket(start, end, totalBase);
    List<Bucket> buckets = HistogramUtil.splitBucket(histogram, columnStatsList, bucket, interval,
        isPureAscii, maxLength);
    assertEquals(3, buckets.size());
  }

  @Test
  public void testRefineToEquiDepth() {
    BigDecimal avgCount = totalCount.divide(BigDecimal.valueOf(21), MathContext.DECIMAL128);
    HistogramUtil.refineToEquiDepth(histogram, avgCount, columnStatsList, isPureAscii, maxLength);
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
    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2));
    Tuple result = HistogramUtil.increment(sortSpecs, columnStatsList, tuple, totalBase, 1,
        isPureAscii, maxLength);

    Tuple diff = HistogramUtil.diff(histogram.comparator,
        sortSpecs, columnStatsList, tuple, result, isPureAscii, maxLength);
    assertEquals(totalBase, diff);

    diff = HistogramUtil.diff(histogram.comparator,
        sortSpecs, columnStatsList, result, tuple, isPureAscii, maxLength);
    assertEquals(totalBase, diff);
  }

  @Test
  public void testDiff2() {
    SortSpec[] sortSpecs = new SortSpec[2];
    sortSpecs[0] = new SortSpec(schema.getColumn(0), false, true);
    sortSpecs[1] = new SortSpec(schema.getColumn(1));

    Tuple tuple = getVTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2));
    Tuple result = HistogramUtil.increment(sortSpecs, columnStatsList, tuple, totalBase, 1,
        isPureAscii, maxLength);

    Tuple diff = HistogramUtil.diff(histogram.comparator,
        sortSpecs, columnStatsList, tuple, result, isPureAscii, maxLength);
    assertEquals(totalBase, diff);

    diff = HistogramUtil.diff(histogram.comparator,
        sortSpecs, columnStatsList, result, tuple, isPureAscii, maxLength);
    assertEquals(totalBase, diff);
  }
}
