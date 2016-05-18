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

package org.apache.tajo.storage.http;

import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.error.Errors.ResultCode;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.FormatProperty;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.Pair;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.*;

/**
 *
 */
public class ExampleHttpFileTablespace extends Tablespace {
  private static final Log LOG = LogFactory.getLog(ExampleHttpFileTablespace.class);

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Tablespace properties
  //////////////////////////////////////////////////////////////////////////////////////////////////
  private static final StorageProperty STORAGE_PROPERTY =
      new StorageProperty(
          BuiltinStorages.JSON, // default format is json
          false,                // is not movable
          false,                // is not writable
          true,                 // allow arbitrary path
          false                 // doesn't provide metadata
      );

  private static final FormatProperty FORMAT_PROPERTY =
      new FormatProperty(
          false,  // doesn't support insert
          false,  // doesn't support direct insert
          false   // doesn't support result staging
      );

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Configurations
  //////////////////////////////////////////////////////////////////////////////////////////////////
  private static final String TEMP_DIR = "temp_dir";
  private static final String DEFAULT_TEMP_DIR = "file:///tmp/tajo-${user}/http-example/";

  private static final String CLEAR_TEMP_DIR_ON_EXIT = "clear_temp_dir_on_exit";
  private static final String DEFAULT_CLEAR_TEMP_DIR_ON_EXIT = "true";

  private String tmpDir;
  private boolean cleanOnExit;

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Metadata
  //////////////////////////////////////////////////////////////////////////////////////////////////

  //                    database, table,  uri
  private final Map<Pair<String, String>, URI> tableUriMap = new HashMap<>();

  //                    database, table,  file path
  private final Map<Pair<String, String>, URI> tempFileUriMap = new HashMap<>();

  public ExampleHttpFileTablespace(String name, URI uri, JSONObject config) {
    super(name, uri, config);

    LOG.info("ExampleHttpFileTablespace is initialized for " + uri);

    // ts uri: http://data.githubarchive.org

    // create table github (*) tablespace http_example using json
    //    ('path'='/2015-01-01-15.json.gz','compression.codec'='org.apache.hadoop.io.compress.GzipCodec')
  }

  @Override
  protected void storageInit() throws IOException {
    initProperties();
  }

  private void initProperties() {
    tmpDir = (String) config.getOrDefault(TEMP_DIR, DEFAULT_TEMP_DIR);
    cleanOnExit = Boolean.parseBoolean(
        (String) config.getOrDefault(CLEAR_TEMP_DIR_ON_EXIT, DEFAULT_CLEAR_TEMP_DIR_ON_EXIT));
  }

  @Override
  public long getTableVolume(TableDesc table, Optional<EvalNode> filter) {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(table.getUri().toASCIIString()).openConnection();
      connection.setRequestMethod("HEAD");
      connection.connect();
      return connection.getHeaderFieldLong(Names.CONTENT_LENGTH, -1);
    } catch (IOException e) {
      throw new TajoInternalError(e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  @Override
  public URI getRootUri() {
    return uri;
  }

  @Override
  public URI getTableUri(TableMeta meta, String databaseName, String tableName) {
    String tablespaceUriString = uri.toASCIIString();
    String tablePath = meta.getProperty("path");
    if (!tablespaceUriString.endsWith("/") && !tablePath.startsWith("/")) {
      tablePath = "/" + tablePath;
    }
    return URI.create(tablespaceUriString + tablePath);
  }

  @Override
  public List<Fragment> getSplits(String inputSourceId,
                                  TableDesc tableDesc,
                                  boolean requireSort,
                                  @Nullable EvalNode filterCondition)
      throws IOException, TajoException {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(tableDesc.getUri().toASCIIString()).openConnection();
      connection.setRequestProperty(Names.RANGE, "bytes=0-");
      connection.setRequestMethod("HEAD");
      connection.connect();
      int responseCode = connection.getResponseCode();

      long tableVolume = getTableVolume(tableDesc, Optional.empty());
      List<Fragment> fragments = new ArrayList<>();

      switch (responseCode) {

        case HttpURLConnection.HTTP_OK:
          fragments.add(
              new ExampleHttpFileFragment(tableDesc.getUri(), inputSourceId, 0, tableVolume, tmpDir, cleanOnExit));
          break;

        case HttpURLConnection.HTTP_PARTIAL:
          final long defaultTaskSize = conf.getLongVar(ConfVars.TASK_DEFAULT_SIZE);
          for (long firstBytePos = 0; firstBytePos < tableVolume; firstBytePos += defaultTaskSize) {
            final long taskSize = Math.min(tableVolume - firstBytePos, defaultTaskSize);
            final long lastBytePos = firstBytePos + taskSize;
            fragments.add(
                new ExampleHttpFileFragment(tableDesc.getUri(), inputSourceId, firstBytePos, lastBytePos, tmpDir,
                    cleanOnExit));
          }
          break;

        default:
          throw new TajoInternalError("Unexpected HTTP response: " + responseCode);
      }

      return fragments;

    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  @Override
  public StorageProperty getProperty() {
    return STORAGE_PROPERTY;
  }

  @Override
  public FormatProperty getFormatProperty(TableMeta meta) {
    return FORMAT_PROPERTY;
  }

  @Override
  public void close() {

  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext,
                                          TableDesc tableDesc,
                                          Schema inputSchema,
                                          SortSpec[] sortSpecs,
                                          TupleRange dataRange) throws IOException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) throws TajoException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws TajoException, IOException {
    if (isValidTableUri(uri, tableDesc.getUri())) {
      String[] identifiers = IdentifierUtil.splitTableName(tableDesc.getName());
      tableUriMap.put(new Pair<>(identifiers[0], identifiers[1]), tableDesc.getUri());
    } else {
      throw new TajoException(
          ResultCode.INVALID_TABLESPACE_URI, uri.toASCIIString(), tableDesc.getUri().toASCIIString());
    }
  }

  private static boolean isValidTableUri(URI tablespaceUri, URI tableUri) {
//    String tableUriString = tableUri.toASCIIString();
//    if (tablespaceUri.toASCIIString().contains(tableUriString)) {
//      // Table URI should be a full path to a file
//      if (tableUriString.charAt(tableUriString.length() - 1) != '/') {
//        return true;
//      }
//    }
//    return false;
    return true;
  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException, TajoException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void prepareTable(LogicalNode node) throws IOException, TajoException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public Path commitTable(OverridableConf queryContext,
                          ExecutionBlockId finalEbId,
                          LogicalPlan plan,
                          Schema schema,
                          TableDesc tableDesc) throws IOException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void rollbackTable(LogicalNode node) throws IOException, TajoException {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
    throw new TajoRuntimeException(new UnsupportedException());
  }
}
