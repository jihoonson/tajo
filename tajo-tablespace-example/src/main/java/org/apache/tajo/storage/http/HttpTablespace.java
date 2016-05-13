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

import com.google.common.base.Preconditions;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.FormatProperty;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class HttpTablespace extends Tablespace {
  private static final Log LOG = LogFactory.getLog(HttpTablespace.class);

  public static final String OAUTH_PROPERTIES = "oauth_properties";
  protected Properties oauthProperties = new Properties();

  public HttpTablespace(String name, URI uri, JSONObject config) {
    super(name, uri, config);

    initOauthProperties();
  }

  @Override
  protected void storageInit() throws IOException {

  }

  private void initOauthProperties() {
    Object connPropertiesObjects = config.get(OAUTH_PROPERTIES);
    if (connPropertiesObjects != null) {
      Preconditions.checkState(connPropertiesObjects instanceof JSONObject, "Invalid oauth_properties field in configs");
      JSONObject connProperties = (JSONObject) connPropertiesObjects;

      for (Map.Entry<String, Object> entry : connProperties.entrySet()) {
        this.oauthProperties.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public long getTableVolume(TableDesc table, Optional<EvalNode> filter) throws UnsupportedException {
    throw new UnsupportedException();
  }

  @Override
  public URI getTableUri(String databaseName, String tableName) {
    return null;
  }

  @Override
  public List<Fragment> getSplits(String inputSourceId, TableDesc tableDesc, @Nullable EvalNode filterCondition) throws IOException, TajoException {
    return null;
  }

  @Override
  public StorageProperty getProperty() {
    return null;
  }

  @Override
  public FormatProperty getFormatProperty(TableMeta meta) {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc, Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange) throws IOException {
    return new TupleRange[0];
  }

  @Override
  public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) throws TajoException {

  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws TajoException, IOException {

  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException, TajoException {

  }

  @Override
  public void prepareTable(LogicalNode node) throws IOException, TajoException {

  }

  @Override
  public Path commitTable(OverridableConf queryContext, ExecutionBlockId finalEbId, LogicalPlan plan, Schema schema, TableDesc tableDesc) throws IOException {
    return null;
  }

  @Override
  public void rollbackTable(LogicalNode node) throws IOException, TajoException {

  }

  @Override
  public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
    return null;
  }
}
