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

import org.apache.tajo.catalog.TupleRange;
import org.apache.tajo.catalog.statistics.Histogram;
import org.apache.tajo.querymaster.MasterFreqHistogram.BucketWithLocation;
import org.apache.tajo.querymaster.Task.PullHost;

import java.util.ArrayList;
import java.util.List;

public class MasterFreqHistogram extends Histogram<TupleRange, BucketWithLocation> {

  @Override
  public void updateBucket(TupleRange range, double change) {
    
  }

  @Override
  public BucketWithLocation getBucket(TupleRange range) {
    return null;
  }

  public class BucketWithLocation {
    private final TupleRange key;
    private double count; // can be a floating point number
    private final List<PullHost> hosts = new ArrayList<>();

    public BucketWithLocation(TupleRange key, double count, String host, int port) {
      this.key = key;
      this.count = count;
      hosts.add(new PullHost(host, port));
    }
  }
}
