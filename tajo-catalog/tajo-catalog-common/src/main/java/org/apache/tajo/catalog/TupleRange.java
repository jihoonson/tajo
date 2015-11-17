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
  private Tuple end; // usually exclusive
  private final Comparator<Tuple> comp;

  public TupleRange(final Tuple start, final Tuple end, final Comparator<Tuple> comp) {
    this.comp = comp;
    // if there is only one value, start == end
    this.start = start;
    this.end = end;
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

  public boolean include(Tuple tuple) {
    if (start.equals(end)) {
      return start.equals(tuple);
    } else {
      return comp.compare(start, tuple) <= 0
          && comp.compare(end, tuple) > 0;
    }
  }

  public String toString() {
    return "[" + this.start + ", " + this.end + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(start, end);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TupleRange) {
      TupleRange other = (TupleRange) obj;
      return this.start.equals(other.start) &&
          this.end.equals(other.end);
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