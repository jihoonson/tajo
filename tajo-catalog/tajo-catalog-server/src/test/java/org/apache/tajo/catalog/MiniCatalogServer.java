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

import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.*;
import org.apache.tajo.util.CommonTestingUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;

public class MiniCatalogServer extends CatalogServer {

  private TajoConf conf;
  private String testDir;
  private CatalogService catalog;

  public MiniCatalogServer() throws IOException {
    super();
    initAndStart();
  }

  public MiniCatalogServer(Set<MetadataProvider> metadataProviders, Collection<FunctionDesc> sqlFuncs)
      throws IOException {
    super(metadataProviders, sqlFuncs);
    initAndStart();
  }

  public String getTestDir() {
    return testDir;
  }

  private void initAndStart() {
    try {
      testDir = CommonTestingUtil.getTestDir().toString();
      conf = CatalogTestingUtil.configureCatalog(new TajoConf(), testDir);
      this.init(conf);
      this.start();
      catalog = new LocalCatalogWrapper(this);
      if (!catalog.existTablespace(TajoConstants.DEFAULT_TABLESPACE_NAME)) {
        catalog.createTablespace(TajoConstants.DEFAULT_TABLESPACE_NAME, testDir.toString());
      }
      if (!catalog.existDatabase(DEFAULT_DATABASE_NAME)) {
        catalog.createDatabase(DEFAULT_DATABASE_NAME, TajoConstants.DEFAULT_TABLESPACE_NAME);
      }

      for (String table : catalog.getAllTableNames(DEFAULT_DATABASE_NAME)) {
        catalog.dropTable(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, table));
      }
    } catch (UndefinedDatabaseException
        | UndefinedTableException
        | DuplicateDatabaseException
        | UnsupportedCatalogStore
        | IOException
        | InsufficientPrivilegeException
        | DuplicateTablespaceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
    CommonTestingUtil.cleanupTestDir(testDir);
  }

  public CatalogService getCatalogService() {
    return catalog;
  }
}
