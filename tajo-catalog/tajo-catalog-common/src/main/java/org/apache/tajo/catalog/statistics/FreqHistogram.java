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

import com.google.protobuf.ByteString;
import org.apache.tajo.catalog.BaseTupleComparator;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TupleRange;
import org.apache.tajo.catalog.proto.CatalogProtos.FreqBucketProto;
import org.apache.tajo.catalog.proto.CatalogProtos.FreqHistogramProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.TreeSet;

/**
 * Frequency histogram
 */
public class FreqHistogram extends Histogram implements ProtoObject<FreqHistogramProto>, Cloneable {

  public FreqHistogram(SortSpec[] sortSpecs) {
    super(sortSpecs);
  }

  public FreqHistogram(SortSpec[] sortSpec, List<FreqBucket> buckets) {
    this(sortSpec);
    this.buckets.addAll(buckets);
  }

  public FreqHistogram(FreqHistogramProto proto) {
    SortSpec[] sortSpecs = new SortSpec[proto.getSortSpecCount()];
    for (int i = 0; i < sortSpecs.length; i++) {
      sortSpecs[i] = new SortSpec(proto.getSortSpec(i));
    }
    Schema keySchema = HistogramUtil.sortSpecsToSchema(sortSpecs);
    this.sortSpecs = sortSpecs;
    this.comparator = new BaseTupleComparator(keySchema, sortSpecs);
    buckets = new TreeSet<>();
    for (FreqBucketProto eachProto : proto.getBucketsList()) {
      buckets.add(new FreqBucket(eachProto));
    }
  }

  /**
   *
   * @param key
   * @param change
   */
  public void updateBucket(TupleRange key, double change) {
    buckets.add(new FreqBucket(key, change));
  }

  @Override
  public FreqHistogramProto getProto() {
    FreqHistogramProto.Builder builder = FreqHistogramProto.newBuilder();
    for (SortSpec sortSpec : sortSpecs) {
      builder.addSortSpec(sortSpec.getProto());
    }
    for (Bucket bucket : buckets) {
      builder.addBuckets(((FreqBucket)bucket).getProto());
    }
    return builder.build();
  }

  public FreqBucket createBucket(TupleRange key, double card) {
    return new FreqBucket(key, card);
  }

  public class FreqBucket extends Bucket
      implements ProtoObject<FreqBucketProto>, Cloneable {

    public FreqBucket(TupleRange key, double count) {
      super(key, count);
    }

    public FreqBucket(FreqBucketProto proto) {
      int len = proto.getStartKeyCount();
      Tuple startKey = new VTuple(len);
      Tuple endKey = new VTuple(len);
      for (int i = 0; i < len; i++) {
        startKey.put(i, proto.getStartKey(i).size() == 0 ? NullDatum.get() :
            DatumFactory.createFromBytes(sortSpecs[i].getSortKey().getDataType(),
            proto.getStartKey(i).toByteArray()));
        endKey.put(i, proto.getEndKey(i).size() == 0 ? NullDatum.get() :
            DatumFactory.createFromBytes(sortSpecs[i].getSortKey().getDataType(),
            proto.getEndKey(i).toByteArray()));
      }
      this.key = new TupleRange(startKey, endKey, proto.getEndKeyInclusive(), comparator);
      this.card = proto.getCount();
    }

    @Override
    public FreqBucketProto getProto() {
      FreqBucketProto.Builder builder = FreqBucketProto.newBuilder();
      for (int i = 0; i < sortSpecs.length; i++) {
        builder.addStartKey(ByteString.copyFrom(key.getStart().asDatum(i).asByteArray()));
        builder.addEndKey(ByteString.copyFrom(key.getEnd().asDatum(i).asByteArray()));
      }
      builder.setEndKeyInclusive(key.isEndInclusive());
      return builder.setCount(card).build();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      return super.clone();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof FreqBucket) {
        FreqBucket other = (FreqBucket) o;
        return TUtil.checkEquals(this.key, other.key) &&
            this.card == other.card;
      }
      return false;
    }
  }
}
