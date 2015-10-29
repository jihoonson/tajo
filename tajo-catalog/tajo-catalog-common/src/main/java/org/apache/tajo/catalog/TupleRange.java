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
import com.google.common.base.Preconditions;
import org.apache.tajo.storage.Tuple;

import java.util.Comparator;

/**
 * It represents a pair of start and end tuples.
 */
public class TupleRange implements Comparable<TupleRange>, Cloneable {
  private Tuple start;
  private Tuple end;
  private Tuple base;
  private final Comparator<Tuple> comp;

  public TupleRange(final Tuple start, final Tuple end, final Tuple base, final Comparator<Tuple> comp) {
    this.comp = comp;
    // if there is only one value, start == end
    this.start = start;
    this.base = base;
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

  public Tuple getBase() {
    return base;
  }

  public void setBase(Tuple base) {
    this.base = base;
  }

  public TupleRange add(TupleRange other) {
    Preconditions.checkArgument(base.equals(other.getBase()));
    Preconditions.checkArgument(TupleRangeUtil.diff(this, other).equals(base));

    if (this.compareTo(other) < 0) {
      return new TupleRange(this.start, other.end, this.base, comp);
    } else {
      return new TupleRange(other.start, this.end, this.base, comp);
    }
  }

  public String toString() {
    return "[" + this.start + ", " + this.end + ", base: " + this.base + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(start, end, base);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TupleRange) {
      TupleRange other = (TupleRange) obj;
      return this.start.equals(other.start) &&
          this.end.equals(other.end) &&
          this.base.equals(other.base);
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
      cmpVal = comp.compare(this.end, o.end);
      return cmpVal != 0 ? cmpVal : comp.compare(this.base, o.base);
    }
  }

  @Override
  public TupleRange clone() throws CloneNotSupportedException {
    TupleRange newRange = (TupleRange) super.clone();
    newRange.setStart(start.clone());
    newRange.setEnd(end.clone());
    newRange.setBase(base.clone());
    return newRange;
  }
}