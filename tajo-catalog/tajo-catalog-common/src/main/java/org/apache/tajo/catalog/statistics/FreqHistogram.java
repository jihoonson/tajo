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
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FreqBucketProto;
import org.apache.tajo.catalog.proto.CatalogProtos.FreqHistogramProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;

/**
 * Frequency histogram
 */
public class FreqHistogram implements ProtoObject<FreqHistogramProto>, Cloneable, GsonObject {
  protected final Schema keySchema; // TODO: remove
  protected final SortSpec[] sortSpecs;
  protected final Map<TupleRange, Bucket> buckets = new HashMap<>();
  protected final TupleComparator comparator;

  public FreqHistogram(Schema keySchema, SortSpec[] sortSpec) {
    this.keySchema = keySchema;
    this.sortSpecs = sortSpec;
    this.comparator = new BaseTupleComparator(keySchema, sortSpec);
  }

  public FreqHistogram(Schema keySchema, SortSpec[] sortSpec, Collection<Bucket> buckets) {
    this(keySchema, sortSpec);
    for (Bucket bucket : buckets) {
      this.buckets.put(bucket.key, bucket);
    }
  }

  public FreqHistogram(FreqHistogramProto proto) {
    keySchema = new Schema(proto.getSchema());
    sortSpecs = new SortSpec[proto.getSortSpecCount()];
    for (int i = 0; i < sortSpecs.length; i++) {
      sortSpecs[i] = new SortSpec(proto.getSortSpec(i));
    }
    this.comparator = new BaseTupleComparator(keySchema, sortSpecs);
    for (FreqBucketProto eachBucketProto : proto.getBucketsList()) {
      Bucket bucket = new Bucket(eachBucketProto);
      buckets.put(bucket.key, bucket);
    }
  }

  public Schema getKeySchema() {
    return keySchema;
  }

  public SortSpec[] getSortSpecs() {
    return sortSpecs;
  }

  public void updateBucket(Tuple startKey, Tuple endKey, Tuple base, long change) {
    Tuple startClone, endClone, baseClone;
    try {
      startClone = startKey.clone();
      endClone = endKey.clone();
      baseClone = base.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    TupleRange key = new TupleRange(startClone, endClone, baseClone, comparator);
    updateBucket(key, change);
  }

  /**
   *
   * @param key
   * @param change
   */
  public void updateBucket(TupleRange key, long change) {
    if (buckets.containsKey(key)) {
      getBucket(key).incCount(change);
    } else {
      buckets.put(key, new Bucket(key, change));
    }
  }

  /**
   *
   * @param other
   */
  public void merge(FreqHistogram other) {
    for (Bucket eachBucket: other.getAllBuckets()) {
      updateBucket(eachBucket.key, eachBucket.count);
    }
  }

  public Bucket getBucket(Tuple startKey, Tuple endKey, Tuple base) {
    return getBucket(new TupleRange(startKey, endKey, base, comparator));
  }

  protected Bucket getBucket(TupleRange key) {
    return buckets.get(key);
  }

  public Collection<Bucket> getAllBuckets() {
    return Collections.unmodifiableCollection(buckets.values());
  }

  public int size() {
    return buckets.size();
  }

  public Comparator<Tuple> getComparator() {
    return comparator;
  }

  /**
   * Normalize ranges of buckets into the range of [0, 1).
   *
   * @param totalRange
   * @return
   */
  public FreqHistogram normalize(TupleRange totalRange) {
    // TODO: Type must be able to contain BigDecimal
    Schema normalizedSchema = new Schema(new Column[]{new Column("normalized", Type.FLOAT8)});
    SortSpec[] normalizedSortSpecs = new SortSpec[1];
    normalizedSortSpecs[0] = new SortSpec(normalizedSchema.getColumn(0), true, false);
    FreqHistogram normalized = new FreqHistogram(normalizedSchema, normalizedSortSpecs);

    BigDecimal totalDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(sortSpecs, totalRange, true));
    BigDecimal baseDiff = BigDecimal.ONE;

    MathContext mathContext = new MathContext(20, RoundingMode.HALF_DOWN);
    Tuple normalizedBase = new VTuple(normalizedSchema.size());
    BigDecimal div = baseDiff.divide(totalDiff, mathContext);
    normalizedBase.put(0, DatumFactory.createFloat8(div.doubleValue()));

    for (Bucket eachBucket: buckets.values()) {
      BigDecimal startDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(sortSpecs,
          totalRange.getStart(), eachBucket.getStartKey(), totalRange.getBase(), true).subtract(BigInteger.ONE));
      BigDecimal endDiff = new BigDecimal(TupleRangeUtil.computeCardinalityForAllColumns(sortSpecs,
          totalRange.getStart(), eachBucket.getEndKey(), totalRange.getBase(), true).subtract(BigInteger.ONE));
      Tuple normalizedStartTuple = new VTuple(new Datum[]{DatumFactory.createFloat8(startDiff.divide(totalDiff, mathContext).doubleValue())});
      Tuple normalizedEndTuple = new VTuple(new Datum[]{DatumFactory.createFloat8(endDiff.divide(totalDiff, mathContext).doubleValue())});
      normalized.updateBucket(normalizedStartTuple, normalizedEndTuple, normalizedBase, eachBucket.count);
    }

    return normalized;
  }

  @Override
  public String toJson() {
    return null;
  }

  @Override
  public FreqHistogramProto getProto() {
    FreqHistogramProto.Builder builder = FreqHistogramProto.newBuilder();
    builder.setSchema(keySchema.getProto());
    for (Bucket bucket : buckets.values()) {
      builder.addBuckets(bucket.getProto());
    }
    return builder.build();
  }

  public void clear() {
    buckets.clear();
  }

  public Bucket createBucket(TupleRange key, long count) {
    return new Bucket(key, count);
  }

  public class Bucket implements ProtoObject<FreqBucketProto>, Cloneable, GsonObject {
    // start and end keys are inclusive
    private final TupleRange key;
    private long count;

    public Bucket(TupleRange key) {
      this(key, 0);
    }

    public Bucket(TupleRange key, long count) {
      this.key = key;
      this.count = count;
    }

    public Bucket(FreqBucketProto proto) {
      Tuple startKey = new VTuple(keySchema.size());
      Tuple endKey = new VTuple(keySchema.size());
      Tuple base = new VTuple(keySchema.size());
      for (int i = 0; i < keySchema.size(); i++) {
        startKey.put(i, DatumFactory.createFromBytes(keySchema.getColumn(i).getDataType(),
            proto.getStartKey(i).toByteArray()));
        endKey.put(i, DatumFactory.createFromBytes(keySchema.getColumn(i).getDataType(),
            proto.getEndKey(i).toByteArray()));
        base.put(i, DatumFactory.createFromBytes(keySchema.getColumn(i).getDataType(),
            proto.getBase(i).toByteArray()));
      }
      this.key = new TupleRange(startKey, endKey, base, comparator);
      this.count = proto.getCount();
    }

    @Override
    public FreqBucketProto getProto() {
      FreqBucketProto.Builder builder = FreqBucketProto.newBuilder();
      for (int i = 0; i < keySchema.size(); i++) {
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

    public Tuple getStartKey() {
      return key.getStart();
    }

    public Tuple getEndKey() {
      return key.getEnd();
    }

    public long getCount() {
      return count;
    }

    public void setCount(long count) {
      this.count = count;
    }

    public void incCount(long inc) {
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
      this.count += other.count;
    }
  }
}
