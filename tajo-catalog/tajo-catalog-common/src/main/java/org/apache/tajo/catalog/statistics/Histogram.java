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
import org.apache.tajo.storage.Tuple;

import java.util.*;

public abstract class Histogram {

  protected TupleComparator comparator;
  protected SortSpec[] sortSpecs;
  protected Map<TupleRange, Bucket> buckets;

  protected Histogram() {}

  public Histogram(SortSpec[] sortSpecs) {
    Schema keySchema = HistogramUtil.sortSpecsToSchema(sortSpecs);
    this.sortSpecs = sortSpecs;
    this.comparator = new BaseTupleComparator(keySchema, sortSpecs);
    buckets = new TreeMap<>();
  }

  public SortSpec[] getSortSpecs() {
    return sortSpecs;
  }

  public Comparator<Tuple> getComparator() {
    return comparator;
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

  public Bucket getBucket(TupleRange key) {
    return buckets.get(key);
  }

  public List<Bucket> getSortedBuckets() {
    return new LinkedList<>(buckets.values());
  }

  public int size() {
    return buckets.size();
  }

  public void clear() {
    buckets.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Histogram) {
      Histogram other = (Histogram) o;
      boolean eq = Arrays.equals(this.sortSpecs, other.sortSpecs);
      eq &= this.getSortedBuckets().equals(other.getSortedBuckets());
      return eq;
    }
    return false;
  }

  public static abstract class Bucket {
    protected TupleRange key;
    protected double card;

    protected Bucket() {}

    public Bucket(TupleRange key, double card) {
      this.key = key;
      this.card = card;
    }

    public void merge(Bucket other) {
      this.key = TupleRangeUtil.merge(this.key, other.key);
      this.card += other.card;
    }

    public TupleRange getKey() {
      return key;
    }

    public double getCard() {
      return card;
    }

    public void setCard(double card) {
      this.card = card;
    }

    public Tuple getStartKey() {
      return key.getStart();
    }

    public Tuple getEndKey() {
      return key.getEnd();
    }

    public void incCount(double inc) {
      this.card += inc;
    }

    @Override
    public String toString() {
      return key + " (" + card + ")";
    }

    public boolean isEndKeyInclusive() {
      return key.isEndInclusive();
    }

    public void setEndKeyInclusive() {
      this.key.setEndInclusive();
    }
  }
}
