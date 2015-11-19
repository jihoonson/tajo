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

package org.apache.tajo.catalog;

import com.google.common.base.Objects;
import org.apache.tajo.storage.Tuple;

import java.util.Comparator;

/**
 * It represents a pair of start and end tuples.
 */
public class TupleRange implements Comparable<TupleRange>, Cloneable {
  private Tuple start;
  private Tuple end;
  private boolean endInclusive = false;
  private final Comparator<Tuple> comp;

  public TupleRange(final Tuple start, final Tuple end, final Comparator<Tuple> comp) {
    this.comp = comp;
    // if there is only one value, start == end
    this.start = start;
    this.end = end;
  }

  public TupleRange(final Tuple start, final Tuple end, final boolean endInclusive, final Comparator<Tuple> comp) {
    this(start, end, comp);
    this.endInclusive = endInclusive;
  }

  public void setStart(Tuple tuple) {
    this.start = tuple;
  }

  public final Tuple getStart() {
    return this.start;
  }

  public void setEnd(Tuple tuple) {
    this.end = tuple;
  }

  public final Tuple getEnd() {
    return this.end;
  }

  public void setEndInclusive() {
    endInclusive = true;
  }

  public boolean isEndInclusive() {
    return endInclusive;
  }

  public boolean isOverlap(TupleRange other) {
    TupleRange small, large;
    if (comp.compare(this.start, other.start) < 0) {
      small = this;
      large = other;
    } else {
      small = other;
      large = this;
    }
    int compVal = comp.compare(small.getEnd(), large.getStart());
    return compVal > 0
        || (compVal == 0 && small.isEndInclusive());
  }

//  public boolean include(TupleRange other) {
//    if (comp.compare(this.start, other.start) <= 0) {
//      if (other.endInclusive) {
//        return comp.compare(this.end, other.end) > 0;
//      } else {
//        return comp.compare(this.end, other.end) >= 0;
//      }
//    }
//    return false;
//  }

  public String toString() {
    return "[" + this.start + ", " + this.end + (endInclusive ? "]" : ")");
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(start, end, endInclusive);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TupleRange) {
      TupleRange other = (TupleRange) obj;
      return this.start.equals(other.start) &&
          this.end.equals(other.end) &&
          this.endInclusive == other.endInclusive;
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(TupleRange o) {
    // TODO - should handle overlap
    int cmpVal = comp.compare(this.start, o.start);
    if (cmpVal != 0) {
      return cmpVal;
    } else {
      return comp.compare(this.end, o.end);
    }
  }

  @Override
  public TupleRange clone() throws CloneNotSupportedException {
    TupleRange newRange = (TupleRange) super.clone();
    newRange.setStart(start.clone());
    newRange.setEnd(end.clone());
    return newRange;
  }
}