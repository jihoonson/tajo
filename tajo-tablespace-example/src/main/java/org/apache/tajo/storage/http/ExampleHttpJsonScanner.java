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
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.json.JsonLineDeserializer;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.apache.tajo.storage.StorageConstants.DEFAULT_TEXT_ERROR_TOLERANCE_MAXNUM;
import static org.apache.tajo.storage.StorageConstants.TEXT_ERROR_TOLERANCE_MAXNUM;
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
  private final TableMeta tableMeta;
  private final ExampleHttpFileFragment fragment;
  private Schema targets = null;

  private HttpURLConnection connection;

  private VTuple outTuple;
  private TableStats inputStats;

  private long limit;

  private final long startOffset;

  private final long endOffset;

  private JsonLineDeserializer deserializer;

  private int errorPrintOutMaxNum = 5;
  /** Maximum number of permissible errors */
  private final int errorTorrenceMaxNum;
  /** How many errors have occurred? */
  private int errorNum;

  private Status status = Status.NEW;

  public ExampleHttpJsonScanner(Configuration conf, Schema schema, TableMeta tableMeta, Fragment fragment)
      throws IOException {
    this.conf = conf;
    this.schema = schema;
    this.tableMeta = tableMeta;
    this.fragment = (ExampleHttpFileFragment) fragment;
    this.startOffset = this.fragment.getStartKey();
    this.endOffset = this.fragment.getEndKey();
    this.errorTorrenceMaxNum =
        Integer.parseInt(tableMeta.getProperty(TEXT_ERROR_TOLERANCE_MAXNUM, DEFAULT_TEXT_ERROR_TOLERANCE_MAXNUM));
  }

  @Override
  public void init() throws IOException {
    status = Status.INITED;

    URL url = new URL(fragment.getUri().toASCIIString());
    connection = (HttpURLConnection) url.openConnection();
    connection.setRequestProperty(Names.RANGE, getHttpRangeString(fragment));
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
//      URL url = new URL(fragment.getUri().toASCIIString());
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

  private static String getHttpRangeString(ExampleHttpFileFragment fragment) {
    return BYTE_RANGE_PREFIX + fragment.getStartKey() + "-" + (fragment.getEndKey() - 1); // end key is inclusive
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
    status = Status.INITED;
  }

  @Override
  public void close() throws IOException {
    outTuple = null;
    status = Status.CLOSED;
  }

  @Override
  public void pushOperators(LogicalNode planPart) {
    // do nothing
  }

  @Override
  public boolean isProjectable() {
    return true;
  }

  @Override
  public void setTarget(Column[] targets) {
    this.targets = SchemaBuilder.builder().addAll(targets).build();
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setFilter(EvalNode filter) {
    // do nothing
  }

  @Override
  public void setLimit(long num) {
    this.limit = num;
  }

  @Override
  public boolean isSplittable() {
    return true;
  }

  @Override
  public float getProgress() {
    return 0.f;
  }

  @Override
  public TableStats getInputStats() {
    return inputStats;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
