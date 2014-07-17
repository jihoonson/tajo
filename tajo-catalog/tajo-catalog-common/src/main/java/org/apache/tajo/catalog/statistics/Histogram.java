/*
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

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.HistogramEntryProto;
import org.apache.tajo.catalog.proto.CatalogProtos.HistogramProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.json.GsonObject;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

public class Histogram implements ProtoObject<HistogramProto>, Cloneable, GsonObject {
  private final HistogramProto.Builder builder = HistogramProto.newBuilder();
  @Expose private Column column = null;
  @Expose private SortedSet<HistogramEntry> entries =
      new TreeSet<HistogramEntry>(new HistogramEntryComparator());

  public Histogram() {

  }

  public Histogram(Column column) {
    this.column = column;
  }

  public Histogram(HistogramProto proto) {
    this.column = new Column(proto.getColumn());
    for (HistogramEntryProto eachProto : proto.getEntriesList()) {
      entries.add(new HistogramEntry(eachProto));
    }
  }

  public Column getColumn() {
    return column;
  }

  public SortedSet<HistogramEntry> getEntries() {
    return entries;
  }

  public void setColumn(Column column) {
    this.column = column;
  }

  public void addEntry(HistogramEntry entry) {
    this.entries.add(entry);
  }

  public void addEntry(Datum start, Datum end, long count) {
    this.entries.add(new HistogramEntry(start, end, count));
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, Histogram.class);
  }

  @Override
  public HistogramProto getProto() {
    builder.clear();
    if (column != null) {
      builder.setColumn(column.getProto());
    }
    if (entries != null) {
      for (HistogramEntry eachEntry : entries) {
        builder.addEntries(eachEntry.getProto());
      }
    }
    return builder.build();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Histogram clone = (Histogram) super.clone();
    clone.column = new Column(column.getQualifiedName(), column.getDataType());
    for (HistogramEntry eachEntry : entries) {
      clone.entries.add((HistogramEntry) eachEntry.clone());
    }
    return clone;
  }

  public static class HistogramEntryComparator implements Comparator<HistogramEntry> {

    @Override
    public int compare(HistogramEntry entry, HistogramEntry entry2) {
      // assume that ranges are not overlapped
      return entry.start.compareTo(entry2.start);
    }
  }

  public static class HistogramEntry implements ProtoObject<HistogramEntryProto>, Cloneable, GsonObject {
    private final HistogramEntryProto.Builder builder = HistogramEntryProto.newBuilder();
    @Expose private Long count;
    @Expose private Datum start;
    @Expose private Datum end;

    public HistogramEntry() {

    }

    public HistogramEntry(Datum start, Datum end) {
      this.start = start;
      this.end = end;
    }

    public HistogramEntry(Datum start, Datum end, long count) {
      this.start = start;
      this.end = end;
      this.count = count;
    }

    public HistogramEntry(HistogramEntryProto proto) {
      this.count = proto.getCount();
      this.start = CatalogGsonHelper.fromJson(proto.getStartValInJson(), Datum.class);
      this.end = CatalogGsonHelper.fromJson(proto.getEndValInJson(), Datum.class);
    }

    public void setCount(long count) {
      this.count = count;
    }

    public void incCount() {
      this.count++;
    }

    public long getCount() {
      return count;
    }

    public Datum getStart() {
      return start;
    }

    public Datum getEnd() {
      return end;
    }

    @Override
    public String toJson() {
      return CatalogGsonHelper.toJson(this, HistogramEntry.class);
    }

    @Override
    public HistogramEntryProto getProto() {
      builder.clear();
      if (count != null) {
        builder.setCount(count);
      }
      if (start != null) {
        builder.setStartValInJson(start.toJson());
      }
      if (end != null) {
        builder.setEndValInJson(end.toJson());
      }
      return builder.build();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      HistogramEntry clone = (HistogramEntry) super.clone();
      clone.count = count;
      // TODO
      clone.start = start;
      clone.end = end;
      return clone;
    }
  }
}
