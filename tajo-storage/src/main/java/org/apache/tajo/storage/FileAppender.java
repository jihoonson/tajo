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

package org.apache.tajo.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StatType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.*;

public abstract class FileAppender implements Appender {
  protected boolean inited = false;

  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  protected final Path path;

  protected boolean enabledStats;
  protected Map<Integer, Collection<StatType>> columnStatEnabled = TUtil.newHashMap();
  
  public FileAppender(Configuration conf, Schema schema, TableMeta meta, Path path) {
    this.conf = conf;
    this.meta = meta;
    this.schema = schema;
    this.path = path;
  }

  public void init() throws IOException {
    if (inited) {
     throw new IllegalStateException("FileAppender is already initialized.");
    }
    inited = true;
  }

  public void enableStats() {
    if (inited) {
      throw new IllegalStateException("Should enable this option before init()");
    }

    this.enabledStats = true;
  }

  @Override
  public void enableColumnStat(Column column, Collection<StatType> statTypes) {
    if (inited) {
      throw new IllegalStateException("Should enable this option before init()");
    }
    columnStatEnabled.put(schema.getColumnId(column.getQualifiedName()), statTypes);
  }

  @Override
  public void enableAllColumnStats() {
    for (Column col : schema.getColumns()) {
      enableColumnStat(col, getAllColumnStatTypes());
    }
  }

  public long getEstimatedOutputSize() throws IOException {
    return getOffset();
  }

  public abstract long getOffset() throws IOException;

  private static Collection<StatType> getAllColumnStatTypes() {
    Collection<StatType> allStats = new HashSet<StatType>();
    allStats.add(StatType.COLUMN_NUM_DIST_VALS);
    allStats.add(StatType.COLUMN_NUM_NULLS);
    allStats.add(StatType.COLUMN_MIN_VALUE);
    allStats.add(StatType.COLUMN_MAX_VALUE);
    allStats.add(StatType.COLUMN_HISTOGRAM);
    return allStats;
  }
}
