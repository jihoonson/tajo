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
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.FormatProperty;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HttpFileTablespace extends Tablespace {
  private static final Log LOG = LogFactory.getLog(HttpFileTablespace.class);

  private static final StorageProperty STORAGE_PROPERTY =
      new StorageProperty(BuiltinStorages.JSON, false, false, true, false);
  private static final FormatProperty FORMAT_PROPERTY = new FormatProperty(false, false, false);

//  public static final String OAUTH_PROPERTIES = "oauth_properties";
//  protected Properties oauthProperties = new Properties();

  //                    database, table,  uri
  private final Map<Pair<String, String>, URI> tableUriMap = new HashMap<>();

  //                    database, table,  file path
  private final Map<Pair<String, String>, URI> tempFileUriMap = new HashMap<>();

  public HttpFileTablespace(String name, URI uri, JSONObject config) {
    super(name, uri, config);
    LOG.info("HttpFileTablespace is initialized for " + uri);

    // ts uri: http://data.githubarchive.org
    // database name:
    // table name: ${prefix}_${path_without_file_extension}
  }

  @Override
  protected void storageInit() throws IOException {
//    initOauthProperties();
    // TODO: load configs
  }

//  private void initOauthProperties() {
//    Object connPropertiesObjects = config.get(OAUTH_PROPERTIES);
//    if (connPropertiesObjects != null) {
//      Preconditions.checkState(connPropertiesObjects instanceof JSONObject, "Invalid oauth_properties field in configs");
//      JSONObject connProperties = (JSONObject) connPropertiesObjects;
//
//      for (Map.Entry<String, Object> entry : connProperties.entrySet()) {
//        this.oauthProperties.put(entry.getKey(), entry.getValue());
//      }
//    }
//  }

  @Override
  public long getTableVolume(TableDesc table, Optional<EvalNode> filter) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public URI getTableUri(String databaseName, String tableName) {
    return tableUriMap.get(new Pair<>(databaseName, tableName));
  }

  @Override
  public List<Fragment> getSplits(String inputSourceId, TableDesc tableDesc, @Nullable EvalNode filterCondition)
      throws IOException, TajoException {
    // TODO: pick a random host
    // generate a temp directory download a file in the scanner.
    // temp directory name: hash(dbname, tablename)?
    // scanner should receive the path to the file not directory.
    return null;
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
    // check clear_temp_dir_on_exit
    // delete temp files
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
    // TODO: maybe keep table uris in map. Otherwise, a catalog instance must be passed to Tablespace.
    // check ts uri
    // register the table uri for dbname and tablename
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
