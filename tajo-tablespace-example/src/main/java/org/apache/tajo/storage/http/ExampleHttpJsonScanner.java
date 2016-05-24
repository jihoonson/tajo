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

package org.apache.tajo.storage.http;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.apache.tajo.storage.http.ExampleHttpFileTablespace.BYTE_RANGE_PREFIX;

public class ExampleHttpJsonScanner implements Scanner {

  private static final Log LOG = LogFactory.getLog(ExampleHttpJsonScanner.class);

  private enum Status {
    NEW,
    INITED,
    CLOSED,
  }

  private final Configuration conf;
  private final Schema schema;
  private final TableMeta httpTableMeta;
  private final TableMeta fileTableMeta;
  private final ExampleHttpFileFragment httpFragment;
  private Schema targets = null;

  private HttpURLConnection connection;

  private Status status = Status.NEW;

  public ExampleHttpJsonScanner(Configuration conf, Schema schema, TableMeta tableMeta, Fragment fragment)
      throws IOException {
    this.conf = conf;
    this.schema = schema;
    this.httpTableMeta = tableMeta;
    this.fileTableMeta = new TableMeta(BuiltinStorages.JSON, httpTableMeta.getPropertySet());
    this.httpFragment = (ExampleHttpFileFragment) fragment;
  }

  @Override
  public void init() throws IOException {
    status = Status.INITED;

    URL url = new URL(httpFragment.getUri().toASCIIString());
    connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty(Names.RANGE, getHttpRangeString());
  }

  public void prepareDataFile() throws IOException {
//    FileSystem fs = fileFragment.getPath().getFileSystem(conf);
//
//    Path parent = fileFragment.getPath().getParent();
//    if (!fs.exists(parent)) {
//      fs.mkdirs(parent);
//    }
//
//    if (!fs.exists(fileFragment.getPath())) {
//      URL url = new URL(httpFragment.getUri().toASCIIString());
//      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
//      connection.setRequestProperty(Names.RANGE, getHttpRangeString());
//
//      try (ReadableByteChannel rbc = Channels.newChannel(connection.getInputStream());
//           FileOutputStream fos = new FileOutputStream(new File(fileFragment.getPath().toUri()));
//           FileChannel fc = fos.getChannel()) {
//        fc.transferFrom(rbc, 0, fileFragment.getLength());
//      } finally {
//        connection.disconnect();
//
//        LOG.info("Data is prepared at " + fileFragment.getPath());
//      }
//    }
  }

  private String getHttpRangeString() {
    return BYTE_RANGE_PREFIX + httpFragment.getStartKey() + "-" + (httpFragment.getEndKey() - 1); // end key is inclusive
  }

  @Override
  public Tuple next() throws IOException {
    if (status != Status.INITED) {
      throw new TajoInternalError("Invalid status:" + status.name());
    }
//    return scanner.next();
    return null;
  }

  @Override
  public void reset() throws IOException {
//    scanner.reset();
    status = Status.INITED;
  }

  @Override
  public void close() throws IOException {
//    scanner.close();
    status = Status.CLOSED;
  }

  @Override
  public void pushOperators(LogicalNode planPart) {
//    scanner.pushOperators(planPart);
  }

  @Override
  public boolean isProjectable() {
//    return scanner.isProjectable();
    return true;
  }

  @Override
  public void setTarget(Column[] targets) {
    this.targets = SchemaBuilder.builder().addAll(targets).build();
  }

  @Override
  public boolean isSelectable() {
//    return scanner.isSelectable();
    return false;
  }

  @Override
  public void setFilter(EvalNode filter) {
//    scanner.setFilter(filter);
  }

  @Override
  public void setLimit(long num) {
//    scanner.setLimit(num);
  }

  @Override
  public boolean isSplittable() {
//    return scanner.isSplittable();
    return true;
  }

  @Override
  public float getProgress() {
//    return scanner.getProgress();
    return 0;
  }

  @Override
  public TableStats getInputStats() {
//    return scanner.getInputStats();
    return null;
  }

  @Override
  public Schema getSchema() {
//    return scanner.getSchema();
    return null;
  }
}
