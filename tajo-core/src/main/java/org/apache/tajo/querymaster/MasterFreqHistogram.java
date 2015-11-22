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
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.querymaster.Task.PullHost;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.Pair;

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

  public void merge(AnalyzedSortSpec[] analyzedSpecs, FreqHistogram other, WorkerConnectionInfo workerInfo) {
    SortedSet<Bucket> thisBuckets = this.getSortedBuckets();
    SortedSet<Bucket> otherBuckets = other.getSortedBuckets();
    Iterator<Bucket> thisIt = thisBuckets.iterator();
    Iterator<Bucket> otherIt = otherBuckets.iterator();

    this.buckets.clear();
    BucketWithLocation thisBucket = null;
    BucketWithLocation otherBucket = null;
    while ((thisBucket != null || thisIt.hasNext())
        && (otherBucket != null || otherIt.hasNext())) {
      if (thisBucket == null) thisBucket = (BucketWithLocation) thisIt.next();
      if (otherBucket == null) otherBucket = new BucketWithLocation(otherIt.next(), workerInfo);

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
//        this.buckets.put(smallStartBucket.getKey(), smallStartBucket);
        this.buckets.add(smallStartBucket);
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

          BucketWithLocation lastBucket = null;
          for (int i = 0; i < bucketOrder.length; i++) {
            Tuple start = tuples[i];
            Tuple end = tuples[i + 1];

            if (i == 1) {
              // Overlapped bucket
              Pair<TupleRange, Double> keyAndCard = HistogramUtil.getSubBucket(this, analyzedSpecs, smallStartBucket,
                  start, end);
              BucketWithLocation smallSubBucket = new BucketWithLocation(keyAndCard.getFirst(), keyAndCard.getSecond(), workerInfo);
              keyAndCard = HistogramUtil.getSubBucket(this, analyzedSpecs, largeStartBucket,
                  start, end);
              BucketWithLocation largeSubBucket = new BucketWithLocation(keyAndCard.getFirst(), keyAndCard.getSecond(), workerInfo);
              try {
                Preconditions.checkState(smallSubBucket.getKey().equals(largeSubBucket.getKey()), "small.key: " + smallStartBucket.getKey() + " large.key: " + largeStartBucket.getKey());
              } catch (IllegalStateException e) {
                throw e;
              }
              smallSubBucket.incCount(largeSubBucket.getCard());
              lastBucket = smallSubBucket;
              // Note: Don't need to consider end inclusive here
              // because this bucket is always a middle one among the split buckets
            } else {
              if (!start.equals(end) || bucketOrder[i].getKey().isEndInclusive()) {
                Pair<TupleRange, Double> keyAndCard = HistogramUtil.getSubBucket(this, analyzedSpecs, bucketOrder[i],
                    start, end);
                BucketWithLocation subBucket = new BucketWithLocation(keyAndCard.getFirst(), keyAndCard.getSecond(), workerInfo);
                if (i > 1 && bucketOrder[i].getKey().isEndInclusive()) {
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
//      this.buckets.put(thisBucket.getKey(), thisBucket);
      this.buckets.add(thisBucket);
    }

    if (otherBucket != null) {
//      this.buckets.put(otherBucket.getKey(), otherBucket);
      this.buckets.add(otherBucket);
    }

    while (thisIt.hasNext()) {
      Bucket next = thisIt.next();
//      this.buckets.put(next.getKey(), next);
      this.buckets.add(next);
    }

    while (otherIt.hasNext()) {
      Bucket next = otherIt.next();
//      this.buckets.put(next.getKey(), new BucketWithLocation(next, workerInfo));
      this.buckets.add(new BucketWithLocation(next, workerInfo));
    }
  }

  public static class BucketWithLocation extends Bucket {
    private final Set<PullHost> hosts = new HashSet<>();

    public BucketWithLocation(TupleRange key, double count, WorkerConnectionInfo info) {
      this(key, count, info.getHost(), info.getPullServerPort());
    }

    public BucketWithLocation(TupleRange key, double count, String host, int port) {
      super(key, count);
      hosts.add(new PullHost(host, port));
    }

    public BucketWithLocation(TupleRange key, double count, Set<PullHost> hosts) {
      super(key, count);
      hosts.addAll(hosts);
    }

    public BucketWithLocation(Bucket bucket, WorkerConnectionInfo info) {
      this(bucket.getKey(), bucket.getCard(), info.getHost(), info.getPullServerPort());
    }

    public BucketWithLocation(Bucket bucket, String host, int port) {
      this(bucket.getKey(), bucket.getCard(), host, port);
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
  }
}
