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

package org.apache.tajo.engine.planner;

import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TupleRange;
import org.apache.tajo.catalog.TupleRangeUtil;

import java.math.BigInteger;

public abstract class RangePartitionAlgorithm {
  protected SortSpec [] sortSpecs;
  protected TupleRange mergedRange;
  protected final BigInteger totalCard; // total cardinality
  /** true if the end of the range is inclusive. Otherwise, it should be false. */
  protected final boolean inclusive;

  /**
   *
   * @param sortSpecs The array of sort keys
   * @param totalRange The total range to be partition
   * @param inclusive true if the end of the range is inclusive. Otherwise, false.
   */
  public RangePartitionAlgorithm(SortSpec [] sortSpecs, TupleRange totalRange, boolean inclusive) {
    this.sortSpecs = sortSpecs;
    try {
      this.mergedRange = totalRange.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    this.inclusive = inclusive;
    this.totalCard = TupleRangeUtil.computeCardinalityForAllColumns(sortSpecs, totalRange, inclusive);
  }

  public BigInteger getTotalCardinality() {
    return totalCard;
  }

  /**
   *
   * @param partNum the number of desired partitions, but it may return the less partitions.
   * @return the end of intermediate ranges are exclusive, and the end of final range is inclusive.
   */
  public abstract TupleRange[] partition(int partNum);
}
