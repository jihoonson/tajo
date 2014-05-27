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

package org.apache.tajo.engine.planner.global;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestDataChannelAnnotator {
  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer sqlAnalyzer;
  private static LogicalPlanner planner;
  private static TPCH tpch;
  private static Session session = LocalTajoTestingUtility.createDummySession();

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    catalog.createTablespace(TajoConstants.DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234");
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, TajoConstants.DEFAULT_TABLESPACE_NAME);

    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }

    // TPC-H Schema for Complex Queries
    String [] tpchTables = {
        "part", "supplier", "partsupp", "nation", "region", "lineitem"
    };
    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();
    for (String table : tpchTables) {
      TableMeta m = CatalogUtil.newTableMeta(StoreType.CSV);
      TableDesc d = CatalogUtil.newTableDesc(
          CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, table), tpch.getSchema(table), m,
          CommonTestingUtil.getTestDir());
      catalog.createTable(d);
    }

    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  @Test
  public void test() throws PlanningException, IOException {
    String sql = "select l_linenumber, sum(l_quantity) from lineitem, part where l_partkey = p_partkey and p_size > 1 group by l_linenumber";
    Expr expr = sqlAnalyzer.parse(sql);
    LogicalPlan planNode = planner.createPlan(session, expr);
    LogicalNode plan = planNode.getRootBlock().getRoot();

    MasterPlan masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), new QueryContext(), planNode);
    GlobalPlanner globalPlanner = new GlobalPlanner(util.getConfiguration(), catalog);
    globalPlanner.build(masterPlan);
  }
}
