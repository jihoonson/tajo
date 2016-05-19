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

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import static org.apache.tajo.storage.http.ExampleHttpFileTablespace.BYTE_RANGE_PREFIX;

public class ExampleHttpFileScanner implements Scanner {

  private enum Status {
    NEW,
    INITED,
    CLOSED,
  }

  private final Configuration conf;
  private final Schema schema;
  private final TableMeta tableMeta;
  private final ExampleHttpFileFragment httpFragment;
  private final FileFragment fileFragment;
  private final Scanner scanner;
  private Schema targets = null;

  private Status status = Status.NEW;

  public ExampleHttpFileScanner(Configuration conf, Schema schema, TableMeta tableMeta, Fragment fragment)
      throws IOException {
    this.conf = conf;
    this.schema = schema;
    this.tableMeta = tableMeta;
    this.httpFragment = (ExampleHttpFileFragment) fragment;
    this.fileFragment = getFileFragment();
    this.scanner = getScanner();
  }

  private Scanner getScanner() throws IOException {
    Tablespace tablespace = TablespaceManager.get(httpFragment.getTempDir());
    return tablespace.getScanner(tableMeta, schema, fileFragment, targets);
  }

  private FileFragment getFileFragment() {
    Path filePath = new Path(httpFragment.getTempDir(), generateFileName(httpFragment));

    return new FileFragment(
        httpFragment.getInputSourceId(),
        filePath,
        httpFragment.getStartKey(),
        httpFragment.getLength());
  }

  static String generateFileName(ExampleHttpFileFragment fragment) {
    String fileName = extractFileName(fragment.getUri());
    String ext;
    String fullExtension = "";

    do {
      ext = FilenameUtils.getExtension(fileName);
      fileName = FilenameUtils.removeExtension(fileName);
      fullExtension = ext + "." + fullExtension;
    } while (!ext.equals(""));

    return fileName
        + "_"
        + fragment.getStartKey()
        + "_"
        + fragment.getEndKey()
        + fullExtension.substring(0, fullExtension.length() - 1);
  }

  private static String extractFileName(URI uri) {
    Path path = new Path(uri);
    return path.getName();
  }

  @Override
  public void init() throws IOException {
    prepareDataFile();
    scanner.init();
    status = Status.INITED;
  }

  public void prepareDataFile() throws IOException {
    FileSystem fs = fileFragment.getPath().getFileSystem(conf);

    if (!fs.exists(fileFragment.getPath())) {
      URL url = new URL(httpFragment.getUri().toASCIIString());
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestProperty(Names.RANGE, getHttpRangeString());

      try (ReadableByteChannel rbc = Channels.newChannel(connection.getInputStream());
           FileOutputStream fos = new FileOutputStream(fileFragment.getPath().toString());
           FileChannel fc = fos.getChannel()) {
        fc.transferFrom(rbc, 0, fileFragment.getLength());
      } finally {
        connection.disconnect();
      }
    }
  }

  private String getHttpRangeString() {
    return BYTE_RANGE_PREFIX + httpFragment.getStartKey() + "-" + (httpFragment.getEndKey() - 1); // end key is inclusive
  }

  @Override
  public Tuple next() throws IOException {
    if (status != Status.INITED) {
      throw new TajoInternalError("Invalid status:" + status.name());
    }
    return scanner.next();
  }

  @Override
  public void reset() throws IOException {
    scanner.reset();
    status = Status.INITED;
  }

  @Override
  public void close() throws IOException {
    scanner.close();

    if (httpFragment.cleanOnExit()) {
      Path path = new Path(httpFragment.getTempDir());
      FileSystem fs = path.getFileSystem(conf);
      fs.delete(path, true);
    }

    status = Status.CLOSED;
  }

  @Override
  public void pushOperators(LogicalNode planPart) {
    scanner.pushOperators(planPart);
  }

  @Override
  public boolean isProjectable() {
    return scanner.isProjectable();
  }

  @Override
  public void setTarget(Column[] targets) {
    this.targets = SchemaBuilder.builder().addAll(targets).build();
  }

  @Override
  public boolean isSelectable() {
    return scanner.isSelectable();
  }

  @Override
  public void setFilter(EvalNode filter) {
    scanner.setFilter(filter);
  }

  @Override
  public void setLimit(long num) {
    scanner.setLimit(num);
  }

  @Override
  public boolean isSplittable() {
    return scanner.isSplittable();
  }

  @Override
  public float getProgress() {
    return scanner.getProgress();
  }

  @Override
  public TableStats getInputStats() {
    return scanner.getInputStats();
  }

  @Override
  public Schema getSchema() {
    return scanner.getSchema();
  }
}
