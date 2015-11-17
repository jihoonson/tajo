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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FreqBucketProto;
import org.apache.tajo.catalog.proto.CatalogProtos.FreqHistogramProto;
import org.apache.tajo.catalog.statistics.FreqHistogram.Bucket;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.TUtil;

import java.math.BigDecimal;
import java.util.*;

/**
 * Frequency histogram
 */
public class FreqHistogram extends Histogram<TupleRange, Bucket>
    implements ProtoObject<FreqHistogramProto>, Cloneable, GsonObject {

  private final TupleComparator comparator;
  private final SortSpec[] sortSpecs;
  private final Map<TupleRange, Bucket> buckets = new HashMap<>();
  private final TupleComparator intervalComparator;
//  private Tuple minInterval;

  public FreqHistogram(SortSpec[] sortSpecs) {
    Schema keySchema = HistogramUtil.sortSpecsToSchema(sortSpecs);
    this.sortSpecs = sortSpecs;
    this.comparator = new BaseTupleComparator(keySchema, sortSpecs);
    SortSpec[] baseSortSpecs = new SortSpec[keySchema.size()];
    for (int i = 0; i < keySchema.size(); i++) {
      baseSortSpecs[i] = new SortSpec(keySchema.getColumn(i), true, sortSpecs[i].isNullFirst());
    }
    this.intervalComparator = new BaseTupleComparator(keySchema, baseSortSpecs);
  }

  public FreqHistogram(SortSpec[] sortSpec, Collection<Bucket> buckets) {
    this(sortSpec);
    for (Bucket bucket : buckets) {
      this.buckets.put(bucket.key, bucket);
    }
  }

  public FreqHistogram(FreqHistogramProto proto) {
    SortSpec[] sortSpecs = new SortSpec[proto.getSortSpecCount()];
    for (int i = 0; i < sortSpecs.length; i++) {
      sortSpecs[i] = new SortSpec(proto.getSortSpec(i));
    }
    Schema keySchema = HistogramUtil.sortSpecsToSchema(sortSpecs);
    this.sortSpecs = sortSpecs;
    this.comparator = new BaseTupleComparator(keySchema, sortSpecs);
    for (FreqBucketProto eachBucketProto : proto.getBucketsList()) {
      Bucket bucket = new Bucket(eachBucketProto);
      buckets.put(bucket.key, bucket);
    }
    SortSpec[] baseSortSpecs = new SortSpec[keySchema.size()];
    for (int i = 0; i < keySchema.size(); i++) {
      baseSortSpecs[i] = new SortSpec(keySchema.getColumn(i), true, sortSpecs[i].isNullFirst());
    }
    this.intervalComparator = new BaseTupleComparator(keySchema, baseSortSpecs);
  }

//  public Tuple getNonZeroMinInterval(AnalyzedSortSpec[] sortSpecs) {
//    if (this.minInterval == null) {
////      Tuple zeroTuple = TupleRangeUtil.createMinBaseTuple(context.sortSpecs);
//      for (Bucket eachBucket : buckets.values()) {
//        if (minInterval == null) {
//          minInterval = eachBucket.getInterval();
//        } else if (!HistogramUtil.isMinNormTuple(HistogramUtil.normalizeTupleAsVector(sortSpecs, eachBucket.getInterval()))
//            && intervalComparator.compare(minInterval, eachBucket.getInterval()) > 0) {
//          minInterval = eachBucket.getInterval();
//        }
//      }
//    }
//    return minInterval;
//  }

  public SortSpec[] getSortSpecs() {
    return sortSpecs;
  }

  public void updateBucket(Tuple startKey, Tuple endKey, double change) {
    // TODO: normalize length
    Tuple startClone, endClone;
    try {
      startClone = startKey.clone();
      endClone = endKey.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    TupleRange key = new TupleRange(startClone, endClone, comparator);
    updateBucket(key, change);
//    minInterval = null;
  }

  /**
   *
   * @param key
   * @param change
   */
  @Override
  public void updateBucket(TupleRange key, double change) {
    if (buckets.containsKey(key)) {
      getBucket(key).incCount(change);
    } else {
      buckets.put(key, new Bucket(key, change));
    }
  }

  public void updateBucket(TupleRange key, double change, boolean endKeyInclusive) {
    updateBucket(key, change);
    buckets.get(key).setEndKeyInclusive(endKeyInclusive);
  }

  /**
   *
   * @param other
   */
  public void merge(AnalyzedSortSpec[] analyzedSpecs, FreqHistogram other) {
    // Find the min interval from both histograms
//    Tuple minInterval;
//    if (this.size() > 0 && other.size() > 0) {
//      minInterval = comparator.compare(this.getNonZeroMinInterval(analyzedSpecs),
//          other.getNonZeroMinInterval(analyzedSpecs)) > 0 ?
//          other.getNonZeroMinInterval(analyzedSpecs) : this.getNonZeroMinInterval(analyzedSpecs);
//    } else if (this.size() > 0) {
//      minInterval = this.getNonZeroMinInterval(analyzedSpecs);
//    } else if (other.size() > 0) {
//      minInterval = other.getNonZeroMinInterval(analyzedSpecs);
//    } else {
//      return;
//    }
//
//    // Split buckets with the min interval
//    this.buckets.clear();
//    List<Bucket> needSplit = new ArrayList<>();
//    for (Bucket eachBucket : this.buckets.values()) {
//      if (comparator.compare(eachBucket.getInterval(), minInterval) > 0) {
//        needSplit.add(eachBucket);
//      } else {
//        this.buckets.put(eachBucket.key, eachBucket);
//      }
//    }
//
//    for (Bucket eachBucket: other.buckets.values()) {
//      if (comparator.compare(eachBucket.getInterval(), minInterval) > 0) {
//        needSplit.add(eachBucket);
//      } else {
//        this.buckets.put(eachBucket.key, eachBucket);
//      }
//    }
//    for (Bucket eachBucket : needSplit) {
//      // Split the bucket
//      for (Bucket split : HistogramUtil.splitBucket(this, analyzedSpecs, eachBucket, minInterval)) {
//        this.buckets.put(split.key, split);
//      }
//    }
//
//    this.minInterval = other.minInterval = minInterval;

    List<Bucket> thisBuckets = this.getSortedBuckets();
    List<Bucket> otherBuckets = other.getSortedBuckets();
    Iterator<Bucket> thisIt = thisBuckets.iterator();
    Iterator<Bucket> otherIt = otherBuckets.iterator();

    this.buckets.clear();
    Bucket thisBucket = null, otherBucket = null;
    while ((thisBucket != null || thisIt.hasNext())
        && (otherBucket != null || otherIt.hasNext())) {
      if (thisBucket == null) thisBucket = thisIt.next();
      if (otherBucket == null) otherBucket = otherIt.next();

      Bucket smallStartBucket, largeStartBucket;
      boolean isThisSmall = thisBucket.key.compareTo(otherBucket.key) < 0;
      if (isThisSmall) {
        smallStartBucket = thisBucket;
        largeStartBucket = otherBucket;
      } else {
        smallStartBucket = otherBucket;
        largeStartBucket = thisBucket;
      }

      // check overlap between keys
      // since end is exclusive, the equal case is also included
      if (comparator.compare(smallStartBucket.key.getEnd(), largeStartBucket.key.getStart()) <= 0) {
        // non-overlap keys
        this.buckets.put(smallStartBucket.key, smallStartBucket);
        if (isThisSmall) {
          thisBucket = null;
        } else {
          otherBucket = null;
        }
      } else {
        // Split buckets into overlapped and non-overlapped portions
        Bucket[] bucketOrder = new Bucket[3];
        bucketOrder[0] = smallStartBucket;

        Tuple[] tuples = new Tuple[4];
        tuples[0] = smallStartBucket.getStartKey();
        tuples[1] = largeStartBucket.getStartKey();

        if (comparator.compare(smallStartBucket.getEndKey(), largeStartBucket.getEndKey()) < 0) {
          tuples[2] = smallStartBucket.getEndKey();
          tuples[3] = largeStartBucket.getEndKey();
          bucketOrder[1] = largeStartBucket;
          bucketOrder[2] = smallStartBucket;
        } else {
          tuples[2] = largeStartBucket.getEndKey();
          tuples[3] = smallStartBucket.getEndKey();
          bucketOrder[1] = smallStartBucket;
          bucketOrder[2] = largeStartBucket;
        }

        for (int i = 0; i < bucketOrder.length; i++) {
          Tuple start = tuples[i];
          Tuple end = tuples[i + 1];
          Bucket subBucket;
          if (i == 1) {
            // Overlapped bucket
            Bucket smallSubBucket = HistogramUtil.getSubBucket(this, analyzedSpecs, smallStartBucket, start, end);
            Bucket largetSubBucket = HistogramUtil.getSubBucket(this, analyzedSpecs, largeStartBucket, start, end);
            assert smallSubBucket.getKey().equals(largetSubBucket.getKey());
            smallSubBucket.incCount(largetSubBucket.getCount());
            subBucket = smallSubBucket;
          } else {
            subBucket = HistogramUtil.getSubBucket(this, analyzedSpecs, bucketOrder[i], start, end);
          }
          this.buckets.put(subBucket.key, subBucket);
        }
      }
    }

    while (thisIt.hasNext()) {
      Bucket next = thisIt.next();
      this.buckets.put(next.key, next);
    }

    while (otherIt.hasNext()) {
      Bucket next = otherIt.next();
      this.buckets.put(next.key, next);
    }
  }

  public Bucket getBucket(Tuple startKey, Tuple endKey) {
    return getBucket(new TupleRange(startKey, endKey, comparator));
  }

  @Override
  public Bucket getBucket(TupleRange key) {
    return buckets.get(key);
  }

  public Collection<Bucket> getAllBuckets() {
    return Collections.unmodifiableCollection(buckets.values());
  }

  public List<Bucket> getSortedBuckets() {
    List<Bucket> bucketList = new ArrayList<>(this.buckets.values());
    bucketList.sort(new Comparator<Bucket>() {
      @Override
      public int compare(Bucket o1, Bucket o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });
    return bucketList;
  }

  public int size() {
    return buckets.size();
  }

  public Comparator<Tuple> getComparator() {
    return comparator;
  }

  public Comparator<Tuple> getVectorComparator() {
    return intervalComparator;
  }

  @Override
  public String toJson() {
    return null;
  }

  @Override
  public FreqHistogramProto getProto() {
    FreqHistogramProto.Builder builder = FreqHistogramProto.newBuilder();
    for (SortSpec sortSpec : sortSpecs) {
      builder.addSortSpec(sortSpec.getProto());
    }
    for (Bucket bucket : buckets.values()) {
      builder.addBuckets(bucket.getProto());
    }
    return builder.build();
  }

  public void clear() {
    buckets.clear();
  }

  public Bucket createBucket(TupleRange key, double amount) {
    return new Bucket(key, amount);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof FreqHistogram) {
      FreqHistogram other = (FreqHistogram) o;
      boolean eq = Arrays.equals(this.sortSpecs, other.sortSpecs);
      eq &= this.getSortedBuckets().equals(other.getSortedBuckets());
      return eq;
    }
    return false;
  }

  public class Bucket implements ProtoObject<FreqBucketProto>, Cloneable, GsonObject {
    // [start key, end key)
    private final TupleRange key;
    private Tuple interval = null;
    private double count; // can be a floating point number
    private boolean endKeyInclusive = false; // set for only the last bucket

    public Bucket(TupleRange key) {
      this(key, 0);
    }

    public Bucket(TupleRange key, double count) {
      this.key = key;
      this.count = count;
    }

    public Bucket(FreqBucketProto proto) {
      Tuple startKey = new VTuple(sortSpecs.length);
      Tuple endKey = new VTuple(sortSpecs.length);
      for (int i = 0; i < sortSpecs.length; i++) {
        startKey.put(i, proto.getStartKey(i).size() == 0 ? NullDatum.get() :
            DatumFactory.createFromBytes(sortSpecs[i].getSortKey().getDataType(),
            proto.getStartKey(i).toByteArray()));
        endKey.put(i, proto.getEndKey(i).size() == 0 ? NullDatum.get() :
            DatumFactory.createFromBytes(sortSpecs[i].getSortKey().getDataType(),
            proto.getEndKey(i).toByteArray()));
      }
      this.key = new TupleRange(startKey, endKey, comparator);
      this.count = proto.getCount();
    }

    @Override
    public FreqBucketProto getProto() {
      FreqBucketProto.Builder builder = FreqBucketProto.newBuilder();
      for (int i = 0; i < sortSpecs.length; i++) {
        builder.addStartKey(ByteString.copyFrom(key.getStart().asDatum(i).asByteArray()));
        builder.addEndKey(ByteString.copyFrom(key.getEnd().asDatum(i).asByteArray()));
      }
      return builder.setCount(count).build();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      return super.clone();
    }

    @Override
    public String toJson() {
      return null;
    }

    public TupleRange getKey() {
      return key;
    }

    public Tuple getInterval(AnalyzedSortSpec[] sortSpecs) {
      if (interval == null) {
        interval = HistogramUtil.diff(sortSpecs, key.getStart(), key.getEnd());
      }
      return interval;
    }

    public Tuple getStartKey() {
      return key.getStart();
    }

    public Tuple getEndKey() {
      return key.getEnd();
    }

    public double getCount() {
      return count;
    }

    public void setCount(double count) {
      this.count = count;
    }

    public void incCount(double inc) {
      this.count += inc;
    }

    public void merge(Bucket other) {
      Tuple minStart, maxEnd;
      if (this.key.compareTo(other.key) < 0) {
        minStart = key.getStart();
        maxEnd = other.key.getEnd();
      } else {
        minStart = other.key.getStart();
        maxEnd = key.getEnd();
      }

      this.key.setStart(minStart);
      this.key.setEnd(maxEnd);
      this.interval = null;
      this.count += other.count;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Bucket) {
        Bucket other = (Bucket) o;
        return TUtil.checkEquals(this.key, other.key) &&
            this.count == other.count;
      }
      return false;
    }

    @Override
    public String toString() {
      return key + " (" + count + ")";
    }

    public boolean isEndKeyInclusive() {
      return endKeyInclusive;
    }

    public void setEndKeyInclusive(boolean endKeyInclusive) {
      this.endKeyInclusive = endKeyInclusive;
    }
  }
}