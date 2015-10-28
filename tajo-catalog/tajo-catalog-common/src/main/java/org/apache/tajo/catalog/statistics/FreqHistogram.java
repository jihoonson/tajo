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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos.FreqBucketProto;
import org.apache.tajo.catalog.proto.CatalogProtos.FreqHistogramProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Pair;

import java.util.*;

public class FreqHistogram implements ProtoObject<FreqHistogramProto>, Cloneable, GsonObject {
  protected Schema keySchema;
  protected Map<Pair<Tuple, Tuple>, Bucket> buckets = new HashMap<>();

  public FreqHistogram(Schema keySchema) {
    this.keySchema = keySchema;
  }

  public FreqHistogram(FreqHistogramProto proto) {
    keySchema = new Schema(proto.getSchema());
    for (FreqBucketProto eachBucketProto : proto.getBucketsList()) {
      Bucket bucket = new Bucket(eachBucketProto);
      buckets.put(new Pair<Tuple, Tuple>(bucket.startKey, bucket.endKey), bucket);
    }
  }

  public void updateBucket(Tuple startKey, Tuple endKey, long change) {
    Pair<Tuple, Tuple> key = new Pair<>(startKey, endKey);
    if (buckets.containsKey(key)) {
      getBucket(key).incCount(change);
    } else {
      buckets.put(key, new Bucket(startKey, endKey, change));
    }
  }

  public Bucket getBucket(Tuple startKey, Tuple endKey) {
    return getBucket(new Pair<Tuple, Tuple>(startKey, endKey));
  }

  protected Bucket getBucket(Pair<Tuple, Tuple> key) {
    return buckets.get(key);
  }

  public Collection<Bucket> getAllBuckets() {
    return Collections.unmodifiableCollection(buckets.values());
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


  public class Bucket implements ProtoObject<FreqBucketProto>, Cloneable, GsonObject {
    // start and end keys are inclusive
    private final Tuple startKey;
    private final Tuple endKey;
    private long count;

    public Bucket(Tuple startKey, Tuple endKey) {
      this(startKey, endKey, 0);
    }

    public Bucket(Tuple startKey, Tuple endKey, long count) {
      this.startKey = startKey;
      this.endKey = endKey;
      this.count = count;
    }

    public Bucket(FreqBucketProto proto) {
      this.startKey = new VTuple(keySchema.size());
      this.endKey = new VTuple(keySchema.size());
      for (int i = 0; i < keySchema.size(); i++) {
        startKey.put(i, DatumFactory.createFromBytes(keySchema.getColumn(i).getDataType(), proto.getStartKey(i).toByteArray()));
        endKey.put(i, DatumFactory.createFromBytes(keySchema.getColumn(i).getDataType(), proto.getEndKey(i).toByteArray()));
      }
      this.count = proto.getCount();
    }

    @Override
    public FreqBucketProto getProto() {
      FreqBucketProto.Builder builder = FreqBucketProto.newBuilder();
      for (int i = 0; i < keySchema.size(); i++) {
        builder.addStartKey(ByteString.copyFrom(startKey.asDatum(i).asByteArray()));
        builder.addEndKey(ByteString.copyFrom(endKey.asDatum(i).asByteArray()));
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

    public Tuple getStartKey() {
      return startKey;
    }

    public Tuple getEndKey() {
      return endKey;
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
  }

}
