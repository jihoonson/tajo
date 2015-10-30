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
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class TestFreqHistogram {

  @Test
  public void testNormalize() {
    Schema schema = new Schema();
    schema.addColumn(new Column("col1", Type.FLOAT8));
    schema.addColumn(new Column("col2", Type.INT8));

    SortSpec[] sortSpecs = new SortSpec[2];
    sortSpecs[0] = new SortSpec(schema.getColumn(0));
    sortSpecs[1] = new SortSpec(schema.getColumn(1));

    Tuple totalStart = getTuple(DatumFactory.createFloat8(0.1), DatumFactory.createInt8(2));
    Tuple totalEnd = getTuple(DatumFactory.createFloat8(10.1), DatumFactory.createInt8(202));
    Tuple totalBase = getTuple(DatumFactory.createFloat8(0.5), DatumFactory.createInt8(10));

    TupleComparator comparator = new BaseTupleComparator(schema, sortSpecs);
    TupleRange totalRange = new TupleRange(totalStart, totalEnd, totalBase, comparator);

    Tuple start, end;
    FreqHistogram histogram = new FreqHistogram(schema, sortSpecs);
    for (int i = 0; i < 20; i++) {
      start = getTuple(DatumFactory.createFloat8(0.1 + i * 0.5), DatumFactory.createInt8(2 + i * 10));
      end = getTuple(DatumFactory.createFloat8(0.1 + (i+1) * 0.5), DatumFactory.createInt8(2 + (i+1) * 10));
      histogram.updateBucket(start, end, totalBase, i * 10);
    }

    FreqHistogram normalized = histogram.normalize(totalRange);
    final List<Bucket> buckets = new ArrayList<>(histogram.getAllBuckets());
    buckets.sort(new Comparator<Bucket>() {
      @Override
      public int compare(Bucket o1, Bucket o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });
    List<Bucket> normalizedBuckets = new ArrayList<>(normalized.getAllBuckets());
    normalizedBuckets.sort(new Comparator<Bucket>() {
      @Override
      public int compare(Bucket o1, Bucket o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });
    for (int i = 0; i < buckets.size(); i++) {
      System.out.println(buckets.get(i).getKey() + ": " + buckets.get(i).getCount());
    }

    for (int i = 0; i < buckets.size(); i++) {
      System.out.println(normalizedBuckets.get(i).getKey() + ": " + normalizedBuckets.get(i).getCount());
    }
  }

  private static Tuple getTuple(Datum ... datums) {
    return new VTuple(datums);
  }
}
