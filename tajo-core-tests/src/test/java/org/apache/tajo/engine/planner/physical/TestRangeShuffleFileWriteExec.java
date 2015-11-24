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

package org.apache.tajo.engine.planner.physical;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.FreqHistogram;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.logical.ShuffleFileWriteNode;
import org.apache.tajo.plan.logical.SortNode;
import org.apache.tajo.plan.serder.PlanProto.ShuffleType;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class TestRangeShuffleFileWriteExec {
  private static TajoConf conf;
  private static final String TEST_PATH = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestPhysicalPlanner";
  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static FileTablespace sm;
  private static Path workDir;
  private static Path tablePath;
  private static TableMeta employeeMeta;
  private static QueryContext queryContext;
  private static TableDesc desc;

  private static Random rnd = new Random(System.currentTimeMillis());

  @Before
  public void setup() throws Exception {
    conf = new TajoConf();
    conf.setBoolVar(TajoConf.ConfVars.$TEST_MODE, true);
    conf.setIntVar(ConfVars.$SORT_LIST_SIZE, 100);
    util = TpchTestBase.getInstance().getTestingCluster();
    catalog = util.getMaster().getCatalog();
    workDir = CommonTestingUtil.getTestDir(TEST_PATH);
    sm = TablespaceManager.getLocalFs();

    Schema schema = new Schema();
    schema.addColumn("managerid", Type.INT4);
    schema.addColumn("empid", Type.INT4);
    schema.addColumn("deptname", Type.TEXT);

    employeeMeta = CatalogUtil.newTableMeta("TEXT");

    tablePath = StorageUtil.concatPath(workDir, "employee", "table1");
    sm.getFileSystem().mkdirs(tablePath.getParent());

    Appender appender = ((FileTablespace) TablespaceManager.getLocalFs())
        .getAppender(employeeMeta, schema, tablePath);
    appender.init();
    VTuple tuple = new VTuple(schema.size());
    for (int i = 0; i < 10; i++) {
      tuple.put(new Datum[] {
          DatumFactory.createInt4(rnd.nextInt(5)),
          DatumFactory.createInt4(rnd.nextInt(10)),
          DatumFactory.createText("dept_" + rnd.nextInt(10))});
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    desc = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, employeeMeta,
        tablePath.toUri());
    catalog.createTable(desc);

    queryContext = new QueryContext(conf);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(conf, catalog);
  }

  @Test
  public void testCreateHistogram() throws IOException {
    FileFragment[] frags = FileTablespace.splitNG(conf, "default.employee", employeeMeta, tablePath, Integer.MAX_VALUE);
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext,
        LocalTajoTestingUtility
            .newTaskAttemptId(), new FileFragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    DataChannel channel = new DataChannel(null, null, ShuffleType.RANGE_SHUFFLE);
    channel.setSchema(desc.getSchema());
    channel.setShuffleKeys(desc.getSchema().toArray());
    channel.setShuffleOutputNum(3);
    channel.setDataFormat(BuiltinStorages.RAW);
    ctx.setDataChannel(channel);

    SortSpec[] sortSpecs = new SortSpec[3];
    sortSpecs[0] = new SortSpec(desc.getSchema().getColumn(0));
    sortSpecs[1] = new SortSpec(desc.getSchema().getColumn(1));
    sortSpecs[2] = new SortSpec(desc.getSchema().getColumn(2));

    ScanNode scanNode = new ScanNode(0);
    scanNode.init(desc);
    scanNode.setInSchema(desc.getSchema());
    scanNode.setOutSchema(desc.getSchema());

    SortNode sortNode = new SortNode(1);
    sortNode.setSortSpecs(sortSpecs);
    sortNode.setInSchema(desc.getSchema());
    sortNode.setOutSchema(desc.getSchema());
    sortNode.setChild(scanNode);

//    ShuffleFileWriteNode shuffleFileWriteNode = new ShuffleFileWriteNode(2);
//    shuffleFileWriteNode.setDataFormat(BuiltinStorages.TEXT);
//    shuffleFileWriteNode.setShuffle(ShuffleType.RANGE_SHUFFLE,
//        desc.getSchema().toArray(), 2);
//    shuffleFileWriteNode.setInSchema(desc.getSchema());
//    shuffleFileWriteNode.setChild(sortNode);

    PhysicalPlanner planner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = planner.createPlan(ctx, sortNode);
    exec.init();
    while (exec.next() != null) {

    }
    exec.close();

  }
}
