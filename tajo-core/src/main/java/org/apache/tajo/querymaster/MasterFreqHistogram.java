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
import org.apache.tajo.catalog.statistics.FreqHistogram.FreqBucket;
import org.apache.tajo.catalog.statistics.Histogram;
import org.apache.tajo.catalog.statistics.HistogramUtil;
import org.apache.tajo.querymaster.MasterFreqHistogram.BucketWithLocation;
import org.apache.tajo.querymaster.Task.PullHost;
import org.apache.tajo.storage.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MasterFreqHistogram extends Histogram<BucketWithLocation> {

  public MasterFreqHistogram(SortSpec[] sortSpecs) {
    super(sortSpecs);
  }

  public MasterFreqHistogram(SortSpec[] sortSpecs, List<BucketWithLocation> buckets) {
    super(sortSpecs);
    for (BucketWithLocation eachBucket : buckets) {
      this.buckets.put(eachBucket.getKey(), eachBucket);
    }
  }

  @Override
  public BucketWithLocation createBucket(TupleRange key, double amount) {
    return null;
  }

  public void merge(AnalyzedSortSpec[] analyzedSpecs, FreqHistogram other) {
    List<BucketWithLocation> thisBuckets = this.getSortedBuckets();
    List<FreqBucket> otherBuckets = other.getSortedBuckets();
    Iterator<BucketWithLocation> thisIt = thisBuckets.iterator();
    Iterator<FreqBucket> otherIt = otherBuckets.iterator();

    this.buckets.clear();
    Bucket thisBucket = null, otherBucket = null;
    while ((thisBucket != null || thisIt.hasNext())
        && (otherBucket != null || otherIt.hasNext())) {
      if (thisBucket == null) thisBucket = thisIt.next();
      if (otherBucket == null) otherBucket = otherIt.next();

      FreqBucket smallStartBucket, largeStartBucket;
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
          Bucket[] bucketOrder = new FreqBucket[3];
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

          FreqBucket lastBucket = null;
          for (int i = 0; i < bucketOrder.length; i++) {
            Tuple start = tuples[i];
            Tuple end = tuples[i + 1];

            if (i == 1) {
              // Overlapped bucket
              FreqBucket smallSubBucket = (FreqBucket) HistogramUtil.getSubBucket(this, analyzedSpecs, smallStartBucket,
                  start, end);
              FreqBucket largeSubBucket = (FreqBucket) HistogramUtil.getSubBucket(this, analyzedSpecs, largeStartBucket,
                  start, end);
              try {
                Preconditions.checkState(smallSubBucket.getKey().equals(largeSubBucket.getKey()), "small.key: " + smallStartBucket.key + " large.key: " + largeStartBucket.key);
              } catch (IllegalStateException e) {
                throw e;
              }
              smallSubBucket.incCount(largeSubBucket.getCard());
              lastBucket = smallSubBucket;
              // Note: Don't need to consider end inclusive here
              // because this bucket is always a middle one among the split buckets
            } else {
              if (!start.equals(end) || bucketOrder[i].key.isEndInclusive()) {
                FreqBucket subBucket = (FreqBucket) HistogramUtil.getSubBucket(this, analyzedSpecs, bucketOrder[i],
                    start, end);
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
      FreqBucket next = thisIt.next();
      this.buckets.put(next.key, next);
    }

    while (otherIt.hasNext()) {
      FreqBucket next = otherIt.next();
      this.buckets.put(next.key, next);
    }
  }

  public class BucketWithLocation extends Bucket {
    private final List<PullHost> hosts = new ArrayList<>();

    public BucketWithLocation(TupleRange key, double count, String host, int port) {
      super(key, count);
      hosts.add(new PullHost(host, port));
    }

    public void merge(BucketWithLocation other) {

    }
  }
}
