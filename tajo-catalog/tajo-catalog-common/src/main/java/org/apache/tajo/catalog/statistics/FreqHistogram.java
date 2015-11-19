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

import java.util.*;

/**
 * Frequency histogram
 */
public class FreqHistogram extends Histogram<TupleRange, Bucket>
    implements ProtoObject<FreqHistogramProto>, Cloneable, GsonObject {

  private final TupleComparator comparator;
  private final SortSpec[] sortSpecs;
  private final Map<TupleRange, Bucket> buckets;

  public FreqHistogram(SortSpec[] sortSpecs) {
    Schema keySchema = HistogramUtil.sortSpecsToSchema(sortSpecs);
    this.sortSpecs = sortSpecs;
    this.comparator = new BaseTupleComparator(keySchema, sortSpecs);
    buckets = new TreeMap<>(new Comparator<TupleRange>() {
      @Override
      public int compare(TupleRange o1, TupleRange o2) {
        return o1.compareTo(o2);
      }
    });
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
    buckets = new TreeMap<>(new Comparator<TupleRange>() {
      @Override
      public int compare(TupleRange o1, TupleRange o2) {
        return o1.compareTo(o2);
      }
    });
    for (FreqBucketProto eachBucketProto : proto.getBucketsList()) {
      Bucket bucket = new Bucket(eachBucketProto);
      buckets.put(bucket.key, bucket);
    }
  }

  public SortSpec[] getSortSpecs() {
    return sortSpecs;
  }

  public void updateBucket(Tuple startKey, Tuple endKey, double change) {
    updateBucket(startKey, endKey, change, false);
  }

  public void updateBucket(Tuple startKey, Tuple endKey, double change, boolean endKeyInclusive) {
    // TODO: normalize length
    Tuple startClone, endClone;
    try {
      startClone = startKey.clone();
      endClone = endKey.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    TupleRange key = new TupleRange(startClone, endClone, comparator);
    updateBucket(key, change, endKeyInclusive);
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
    if (endKeyInclusive) {
      buckets.get(key).setEndKeyInclusive();
    }
  }

  public void addBucket(Bucket bucket) {
    if (buckets.containsKey(bucket.key)) {
      throw new RuntimeException("Duplicated bucket");
    }
    buckets.put(bucket.key, bucket);
  }

  public void removeBucket(Bucket bucket) {
    buckets.remove(bucket.key);
  }

  /**
   *
   * @param other
   */
  public void merge(AnalyzedSortSpec[] analyzedSpecs, FreqHistogram other) {
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

      // Check overlap between keys
      if (!smallStartBucket.key.isOverlap(largeStartBucket.key)) {
        // non-overlap keys
        this.buckets.put(smallStartBucket.key, smallStartBucket);
        if (isThisSmall) {
          thisBucket = null;
        } else {
          otherBucket = null;
        }
      } else {

        boolean thisSplittable = HistogramUtil.splittable(analyzedSpecs, smallStartBucket);
        boolean otherSplittable = HistogramUtil.splittable(analyzedSpecs, largeStartBucket);

        // If both buckets are splittable
        if (thisSplittable && otherSplittable) {
          // Split buckets into overlapped and non-overlapped portions
          Bucket[] bucketOrder = new Bucket[3];
          bucketOrder[0] = smallStartBucket;

          Tuple[] tuples = new Tuple[4];
          tuples[0] = smallStartBucket.getStartKey();
          tuples[1] = largeStartBucket.getStartKey();

          if (comparator.compare(smallStartBucket.getEndKey(), largeStartBucket.getEndKey()) < 0) {
            tuples[2] = smallStartBucket.getEndKey();
            tuples[3] = largeStartBucket.getEndKey();
            bucketOrder[1] = smallStartBucket;
            bucketOrder[2] = largeStartBucket;
          } else {
            tuples[2] = largeStartBucket.getEndKey();
            tuples[3] = smallStartBucket.getEndKey();
            bucketOrder[1] = largeStartBucket;
            bucketOrder[2] = smallStartBucket;
          }

          Bucket lastBucket = null;
          for (int i = 0; i < bucketOrder.length; i++) {
            Tuple start = tuples[i];
            Tuple end = tuples[i + 1];

            if (i == 1) {
              // Overlapped bucket
              Bucket smallSubBucket = HistogramUtil.getSubBucket(this, analyzedSpecs, smallStartBucket, start, end);
              Bucket largeSubBucket = HistogramUtil.getSubBucket(this, analyzedSpecs, largeStartBucket, start, end);
              try {
                Preconditions.checkState(smallSubBucket.getKey().equals(largeSubBucket.getKey()), "small.key: " + smallStartBucket.key + " large.key: " + largeStartBucket.key);
              } catch (IllegalStateException e) {
                throw e;
              }
              smallSubBucket.incCount(largeSubBucket.getCount());
              lastBucket = smallSubBucket;
              // Note: Don't need to consider end inclusive here
              // because this bucket is always a middle one among the split buckets
            } else {
              if (!start.equals(end) || bucketOrder[i].key.isEndInclusive()) {
                Bucket subBucket = HistogramUtil.getSubBucket(this, analyzedSpecs, bucketOrder[i], start, end);
                if (i > 1 && bucketOrder[i].key.isEndInclusive()) {
                  subBucket.setEndKeyInclusive();
                }
                lastBucket = subBucket;
              }
            }
          }

          if (thisBucket.getEndKey().equals(lastBucket.getEndKey())) {
            thisBucket = lastBucket;
            otherBucket = null;
          } else {
            thisBucket = null;
            otherBucket = lastBucket;
          }

        } else {
          // Simply merge
          isThisSmall = comparator.compare(thisBucket.getEndKey(), otherBucket.getEndKey()) < 0;
          smallStartBucket.merge(largeStartBucket);

          if (isThisSmall) {
            thisBucket = null;
            otherBucket = smallStartBucket;
          } else {
            thisBucket = smallStartBucket;
            otherBucket = null;
          }
        }
      }
    }

    if (thisBucket != null) {
      this.buckets.put(thisBucket.key, thisBucket);
    }

    if (otherBucket != null) {
      this.buckets.put(otherBucket.key, otherBucket);
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

  public Bucket getBucket(Tuple startKey, Tuple endKey, boolean endInclusive) {
    return getBucket(new TupleRange(startKey, endKey, endInclusive, comparator));
  }

  @Override
  public Bucket getBucket(TupleRange key) {
    return buckets.get(key);
  }

  public Collection<Bucket> getAllBuckets() {
    return Collections.unmodifiableCollection(buckets.values());
  }

  public List<Bucket> getSortedBuckets() {
    return new ArrayList<>(buckets.values());
  }

  public int size() {
    return buckets.size();
  }

  public Comparator<Tuple> getComparator() {
    return comparator;
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
    private final TupleRange key;
    private double count; // can be a floating point number

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
      this.key = new TupleRange(startKey, endKey, proto.getEndKeyInclusive(), comparator);
      this.count = proto.getCount();
    }

    @Override
    public FreqBucketProto getProto() {
      FreqBucketProto.Builder builder = FreqBucketProto.newBuilder();
      for (int i = 0; i < sortSpecs.length; i++) {
        builder.addStartKey(ByteString.copyFrom(key.getStart().asDatum(i).asByteArray()));
        builder.addEndKey(ByteString.copyFrom(key.getEnd().asDatum(i).asByteArray()));
      }
      builder.setEndKeyInclusive(key.isEndInclusive());
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
      boolean endKeyInclusive;
      if (this.key.compareTo(other.key) < 0) {
        minStart = key.getStart();
        maxEnd = other.key.getEnd();
        endKeyInclusive = other.key.isEndInclusive();
      } else {
        minStart = other.key.getStart();
        maxEnd = key.getEnd();
        endKeyInclusive = this.key.isEndInclusive();
      }

      this.key.setStart(minStart);
      this.key.setEnd(maxEnd);
      this.count += other.count;
      if (endKeyInclusive) {
        this.setEndKeyInclusive();
      }
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
      return key.isEndInclusive();
    }

    public void setEndKeyInclusive() {
      this.key.setEndInclusive();
    }
  }
}
