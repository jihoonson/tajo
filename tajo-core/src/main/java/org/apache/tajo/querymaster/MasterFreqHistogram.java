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

package org.apache.tajo.querymaster;

import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TupleRange;
import org.apache.tajo.catalog.statistics.AnalyzedSortSpec;
import org.apache.tajo.catalog.statistics.FreqHistogram;
import org.apache.tajo.catalog.statistics.Histogram;
import org.apache.tajo.catalog.statistics.HistogramUtil;
import org.apache.tajo.querymaster.Task.PullHost;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.StringUtils;

import java.util.*;

public class MasterFreqHistogram extends Histogram {

  public MasterFreqHistogram(SortSpec[] sortSpecs) {
    super(sortSpecs);
  }

  public MasterFreqHistogram(SortSpec[] sortSpecs, Collection<Bucket> buckets) {
    super(sortSpecs);
//    for (Bucket eachBucket : buckets) {
//      this.buckets.put(eachBucket.getKey(), eachBucket);
//    }
    this.buckets.addAll(buckets);
  }

  public void merge(AnalyzedSortSpec[] analyzedSpecs, FreqHistogram other, PullHost pullHost, int maxSize) {
    final boolean mergeWithBucketMerge = this.size() + other.size() <= maxSize;

    List<Bucket> thisBuckets = new ArrayList<>(this.getSortedBuckets());
    List<Bucket> otherBuckets = new ArrayList<>(other.getSortedBuckets());
    Iterator<Bucket> thisIt = thisBuckets.iterator();
    Iterator<Bucket> otherIt = otherBuckets.iterator();

    this.buckets.clear();
    BucketWithLocation thisBucket = null;
    BucketWithLocation otherBucket = null;
    while ((thisBucket != null || thisIt.hasNext())
        && (otherBucket != null || otherIt.hasNext())) {
      if (thisBucket == null) thisBucket = (BucketWithLocation) thisIt.next();
      if (otherBucket == null) otherBucket = new BucketWithLocation(otherIt.next(), pullHost);

      BucketWithLocation smallStartBucket, largeStartBucket;
      boolean isThisSmall = thisBucket.getKey().compareTo(otherBucket.getKey()) < 0;
      if (isThisSmall) {
        smallStartBucket = thisBucket;
        largeStartBucket = otherBucket;
      } else {
        smallStartBucket = otherBucket;
        largeStartBucket = thisBucket;
      }

      // Check overlap between keys
      if (!smallStartBucket.getKey().isOverlap(largeStartBucket.getKey())) {
        // non-overlap keys
        this.buckets.add(smallStartBucket);
        if (isThisSmall) {
          thisBucket = null;
        } else {
          otherBucket = null;
        }
      } else {
        Pair<Bucket, Bucket> result;
        if (mergeWithBucketMerge) {
          result = mergeWithBucketMerge(thisBucket, otherBucket, smallStartBucket, largeStartBucket);
        } else {
          result = mergeWithBucketSplit(analyzedSpecs, pullHost, thisBucket, otherBucket, smallStartBucket, largeStartBucket);
        }
        thisBucket = (BucketWithLocation) result.getFirst();
        otherBucket = (BucketWithLocation) result.getSecond();
      }
    }

    if (thisBucket != null) {
      this.buckets.add(thisBucket);
    }

    if (otherBucket != null) {
      this.buckets.add(otherBucket);
    }

    while (thisIt.hasNext()) {
      Bucket next = thisIt.next();
      this.buckets.add(next);
    }

    while (otherIt.hasNext()) {
      Bucket next = otherIt.next();
      this.buckets.add(new BucketWithLocation(next, pullHost));
    }
  }

  private Pair<Bucket, Bucket> mergeWithBucketMerge(Bucket thisBucket, Bucket otherBucket,
                                                    Bucket smallStartBucket, Bucket largeStartBucket) {
    boolean isThisSmall = comparator.compare(thisBucket.getEndKey(), otherBucket.getEndKey()) < 0;
    smallStartBucket.merge(largeStartBucket);

    if (isThisSmall) {
      thisBucket = null;
      otherBucket = smallStartBucket;
    } else {
      thisBucket = smallStartBucket;
      otherBucket = null;
    }
    return new Pair<>(thisBucket, otherBucket);
  }

  private Pair<Bucket, Bucket> mergeWithBucketSplit(AnalyzedSortSpec[] analyzedSpecs, PullHost pullHost,
                                                    Bucket thisBucket, Bucket otherBucket,
                                                    BucketWithLocation smallStartBucket,
                                                    BucketWithLocation largeStartBucket) {
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

      List<BucketWithLocation> splits = new ArrayList<>();

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

      boolean unsplittable = false;
      for (int i = 0; i < bucketOrder.length; i++) {
        Tuple start = tuples[i];
        Tuple end = tuples[i + 1];

        if (i == 1) {
          // Overlapped bucket
          Pair<TupleRange, Double> keyAndCard = HistogramUtil.getSubBucket(this, analyzedSpecs, smallStartBucket,
              start, end);
          unsplittable = keyAndCard.getFirst().equals(smallStartBucket.getKey());
          if (unsplittable) break;

          BucketWithLocation smallSubBucket = new BucketWithLocation(keyAndCard.getFirst(), keyAndCard.getSecond(), pullHost);
          keyAndCard = HistogramUtil.getSubBucket(this, analyzedSpecs, largeStartBucket,
              start, end);
          unsplittable = keyAndCard.getFirst().equals(largeStartBucket.getKey());
          if (unsplittable) break;

          BucketWithLocation largeSubBucket = new BucketWithLocation(keyAndCard.getFirst(), keyAndCard.getSecond(), pullHost);

          Preconditions.checkState(smallSubBucket.getKey().equals(largeSubBucket.getKey()),
              "smallStartBucket.key: " + smallStartBucket.getKey() + " smallSubBucket.key: " + smallSubBucket.getKey()
                  + " largeStartBucket.key: " + largeStartBucket.getKey() + " largeSubBucket.key: " + largeSubBucket.getKey());

          smallSubBucket.incCount(largeSubBucket.getCard());
          splits.add(smallSubBucket);
          // Note: Don't need to consider end inclusive here
          // because this bucket is always a middle one among the split buckets
        } else {
          if (!start.equals(end) || bucketOrder[i].getKey().isEndInclusive()) {
            Pair<TupleRange, Double> keyAndCard = HistogramUtil.getSubBucket(this, analyzedSpecs, bucketOrder[i],
                start, end);
            unsplittable = keyAndCard.getFirst().equals(bucketOrder[i].getKey());
            if (unsplittable) break;

            BucketWithLocation subBucket = new BucketWithLocation(keyAndCard.getFirst(), keyAndCard.getSecond(), pullHost);
            if (i > 1 && bucketOrder[i].getKey().isEndInclusive()) {
              subBucket.setEndKeyInclusive();
            }
            splits.add(subBucket);
          }
        }
      }

      if (unsplittable) {
        return mergeWithBucketMerge(thisBucket, otherBucket, smallStartBucket, largeStartBucket);

      } else {
        BucketWithLocation lastBucket = splits.remove(splits.size() - 1);

        this.buckets.addAll(splits);

        if (thisBucket.getEndKey().equals(lastBucket.getEndKey())) {
          thisBucket = lastBucket;
          otherBucket = null;
        } else {
          thisBucket = null;
          otherBucket = lastBucket;
        }

        return new Pair<>(thisBucket, otherBucket);
      }
    } else {
      // Simply merge
      return mergeWithBucketMerge(thisBucket, otherBucket, smallStartBucket, largeStartBucket);
    }
  }

  public static class BucketWithLocation extends Bucket {
    private final Set<PullHost> hosts = new HashSet<>();

    public BucketWithLocation(TupleRange key, double count, PullHost host) {
      super(key, count);
      hosts.add(host);
    }

    public BucketWithLocation(TupleRange key, double count, String host, int port) {
      this(key, count, new PullHost(host, port));
    }

    public BucketWithLocation(TupleRange key, double count, Set<PullHost> hosts) {
      super(key, count);
      this.hosts.addAll(hosts);
    }

    public BucketWithLocation(Bucket bucket, PullHost host) {
      this(bucket.getKey(), bucket.getCard(), host);
    }

    public BucketWithLocation(Bucket bucket, String host, int port) {
      this(bucket.getKey(), bucket.getCard(), host, port);
    }

    public void addHosts(Set<PullHost> hosts) {
      this.hosts.addAll(hosts);
    }

    public Set<PullHost> getHosts() {
      return hosts;
    }

    @Override
    public void merge(Bucket other) {
      super.merge(other);
      BucketWithLocation bucket = (BucketWithLocation) other;
      this.hosts.addAll(bucket.hosts);
    }

    @Override
    public String toString() {
      return this.key + ", " + this.card + ", < " + StringUtils.join(hosts) + " >";
    }
  }
}
