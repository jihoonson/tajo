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
import org.apache.tajo.catalog.proto.CatalogProtos.FreqHistogramProto;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestFreqHistogram {
  private Schema schema;
  private SortSpec[] sortSpecs;
  private FreqHistogram histogram;
  private TupleRange totalRange;
  private Tuple totalBase;
  private List<ColumnStats> columnStatsList;
  private BigInteger totalCard;

  @Before
  public void setup() {
//    schema = new Schema();
//    schema.addColumn(new Column("col1", Type.FLOAT8));
//    schema.addColumn(new Column("col2", Type.INT8));
//
//    sortSpecs = new SortSpec[2];
//    sortSpecs[0] = new SortSpec(schema.getColumn(0));
//    sortSpecs[1] = new SortSpec(schema.getColumn(1));
//
//    totalBase = getTuple(DatumFactory.createFloat8(0.5), DatumFactory.createInt8(10));
//
//    TupleComparator comparator = new BaseTupleComparator(schema, sortSpecs);
//    columnStatsList = new ArrayList<>(2);
//    ColumnStats stats = new ColumnStats(schema.getColumn(0));
//    columnStatsList.add(stats);
//    stats = new ColumnStats(schema.getColumn(1));
//    columnStatsList.add(stats);
//
//    Tuple start = getTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2)), end;
//    histogram = new FreqHistogram(sortSpecs);
//    double maxDouble = 0;
//    long maxLong = 0;
//    long card = 0;
//    histogram.updateBucket(getTuple(NullDatum.get(), NullDatum.get()), getTuple(NullDatum.get(), NullDatum.get()), totalBase, 1);
//    for (int i = 0; i < 20; i++) {
//      long count = (i + 1) * 10;
//      end = getTuple(DatumFactory.createFloat8(start.getFloat8(0) + count * 0.5),
//          DatumFactory.createInt8(start.getInt8(1) + count * 10));
//      maxDouble = end.getFloat8(0);
//      maxLong = end.getInt8(1);
//      histogram.updateBucket(start, end, totalBase, count);
//      start = end;
//      card += count;
//    }
//    columnStatsList.get(0).setMinValue(DatumFactory.createFloat8(0.1));
//    columnStatsList.get(0).setMaxValue(DatumFactory.createFloat8(maxDouble));
//    columnStatsList.get(1).setMinValue(DatumFactory.createInt8(2));
//    columnStatsList.get(1).setMaxValue(DatumFactory.createInt8(maxLong));
//
//    Tuple totalStart = getTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2));
//    Tuple totalEnd = getTuple(DatumFactory.createFloat8(maxDouble), DatumFactory.createInt8(maxLong));
//    totalRange = new TupleRange(totalStart, totalEnd, totalBase, comparator);
//    totalCard = BigInteger.valueOf(card);
  }

  @Test
  public void testPBSerde() {
    FreqHistogramProto proto = histogram.getProto();
    FreqHistogram deserialized = new FreqHistogram(proto);
    assertEquals(histogram, deserialized);
  }

//  @Test
//  public void testNormalizeAndDenormalize() {
//    List<Bucket> originalBuckets = histogram.getSortedBuckets();
//    for (int i = 0; i < originalBuckets.size(); i++) {
//      System.out.println(originalBuckets.get(i).getKey() + ": " + originalBuckets.get(i).getCount());
//    }
//
//    FreqHistogram normalized = HistogramUtil.normalize(histogram, totalRange, totalCard);
//    List<Bucket> normalizedBuckets = normalized.getSortedBuckets();
//    for (int i = 0; i < originalBuckets.size(); i++) {
//      System.out.println(normalizedBuckets.get(i).getKey() + ": " + normalizedBuckets.get(i).getCount());
//    }
//
//    assertTrue(normalizedBuckets.get(0).getStartKey().getFloat8(0) >= 0.0d);
//    assertTrue(normalizedBuckets.get(normalizedBuckets.size()-1).getEndKey().getFloat8(0) <= 1.0d);
//
//    FreqHistogram denormalized = HistogramUtil.denormalize(normalized, schema, sortSpecs, columnStatsList, totalRange);
//    List<Bucket> denormalizedBuckets = denormalized.getSortedBuckets();
//
//    for (int i = 0; i < denormalizedBuckets.size(); i++) {
//      System.out.println(denormalizedBuckets.get(i).getKey() + ": " + denormalizedBuckets.get(i).getCount());
//    }
//
//    assertEquals(originalBuckets.size(), denormalizedBuckets.size());
//    for (int i = 0; i < originalBuckets.size(); i++) {
//      assertEquals(originalBuckets.get(i), denormalizedBuckets.get(i));
//    }
//  }

  private static Tuple getTuple(Datum ... datums) {
    return new VTuple(datums);
  }
}
