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

package org.apache.tajo.engine.query;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@NamedTest("TestJoinQuery")
public class TestJoinQuery extends QueryTestCaseBase {

  public TestJoinQuery(String joinOption) {
    super(TajoConstants.DEFAULT_DATABASE_NAME, joinOption);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "false");
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname, "-1");

    testingCluster.setAllTajoDaemonConfValue(
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);

    if (joinOption.indexOf("Hash") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
    }
    if (joinOption.indexOf("Sort") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
          String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(1));
    }
  }

  @Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"Hash"},
        {"Sort"},
    });
  }

  @AfterClass
  public static void classTearDown() {
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname,
        ConfVars.$TEST_BROADCAST_JOIN_ENABLED.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
        ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest(queries = {
    @QuerySpec("select n_name, r_name, n_regionkey, r_regionkey from nation, region order by n_name, r_name"),
    // testCrossJoinWithAsterisk
    @QuerySpec("select region.*, customer.* from region, customer"),
    @QuerySpec("select region.*, customer.* from customer, region"),
    @QuerySpec("select * from customer, region"),
    @QuerySpec("select length(r_comment) as len, *, c_custkey*10 from customer, region order by len,r_regionkey,r_name")
  })
  public void testCrossJoin() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithThetaJoinConditionInWhere() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testInnerJoinWithThetaJoinConditionInWhere() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithThetaJoinConditionInWhere() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testRightOuterJoinWithThetaJoinConditionInWhere() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testWhereClauseJoin1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testWhereClauseJoin2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testWhereClauseJoin3() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testWhereClauseJoin4() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testWhereClauseJoin5() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testWhereClauseJoin6() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testTPCHQ2Join() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinWithMultipleJoinQual1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinWithMultipleJoinQual2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinWithMultipleJoinQual3() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinWithMultipleJoinQual4() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinWithMultipleJoinTypes() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoin1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithConstantExpr1() throws Exception {
    // outer join with constant projections
    //
    // select c_custkey, orders.o_orderkey, 'val' as val from customer
    // left outer join orders on c_custkey = o_orderkey;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithConstantExpr2() throws Exception {
    // outer join with constant projections
    //
    // select c_custkey, o.o_orderkey, 'val' as val from customer left outer join
    // (select * from orders) o on c_custkey = o.o_orderkey
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithConstantExpr3() throws Exception {
    // outer join with constant projections
    //
    // select a.c_custkey, 123::INT8 as const_val, b.min_name from customer a
    // left outer join ( select c_custkey, min(c_name) as min_name from customer group by c_custkey) b
    // on a.c_custkey = b.c_custkey;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithConstantExpr4() throws Exception {
    // outer join with constant projections
    //
    // select
    //   c_custkey,
    //   orders.o_orderkey,
    //   1 as key1
    // from customer left outer join orders on c_custkey = o_orderkey and key1 = 1;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithConstantExpr5() throws Exception {
    // outer join with constant projections
    //
    // select
    //   c_custkey,
    //   orders.o_orderkey,
    //   1 as key1
    // from customer left outer join orders on c_custkey = o_orderkey and key1 = 1;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testRightOuterJoin1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testFullOuterJoin1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinCoReferredEvals1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinCoReferredEvalsWithSameExprs1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinCoReferredEvalsWithSameExprs2() throws Exception {
    // including grouping operator
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testInnerJoinAndCaseWhen() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinsWithCaseWhen() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinsWithCaseWhen2() throws Exception {
    runSimpleTests();
  }

  @Test
  public void testOuterJoinAndCaseWhen1() throws Exception {
    executeDDL("oj_table1_ddl.sql", "table1");
    executeDDL("oj_table2_ddl.sql", "table2");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");
    }
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testInnerJoinWithEmptyTable() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithEmptyTable1() throws Exception {
    /*
    select
      c_custkey,
      empty_orders.o_orderkey,
      empty_orders.o_orderstatus,
      empty_orders.o_orderdate
    from
      customer left outer join empty_orders on c_custkey = o_orderkey
    order by
      c_custkey, o_orderkey;
     */

    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithEmptyTable2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithEmptyTable3() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithEmptyTable4() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithEmptyTable5() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testRightOuterJoinWithEmptyTable1() throws Exception {
    runSimpleTests();
  }

  @Test
  public void testLeftOuterJoinWithEmptySubquery1() throws Exception {
    // Empty Null Supplying table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3", "4|table11-4", "5|table11-5" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 2);

    data = new String[]{ "1|table11-1", "2|table11-2" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data, 2);

    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "2");

      ResultSet res = executeString("select a.id, b.id from table11 a " +
          "left outer join (" +
          "select table12.id from table12 inner join lineitem on table12.id = lineitem.l_orderkey and table12.id > 10) b " +
          "on a.id = b.id order by a.id");

      String expected = "id,id\n" +
          "-------------------------------\n" +
          "1,null\n" +
          "2,null\n" +
          "3,null\n" +
          "4,null\n" +
          "5,null\n";

      assertEquals(expected, resultSetToString(res));
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname,
          ConfVars.$TEST_MIN_TASK_NUM.defaultVal);
      executeString("DROP TABLE table11 PURGE").close();
      executeString("DROP TABLE table12 PURGE").close();
    }
  }

  @Test
  public void testLeftOuterJoinWithEmptySubquery2() throws Exception {
    //Empty Preserved Row table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3", "4|table11-4", "5|table11-5" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 2);

    data = new String[]{ "1|table11-1", "2|table11-2" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data, 2);

    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname, "2");

      ResultSet res = executeString("select a.id, b.id from " +
          "(select table12.id, table12.name, lineitem.l_shipdate " +
          "from table12 inner join lineitem on table12.id = lineitem.l_orderkey and table12.id > 10) a " +
          "left outer join table11 b " +
          "on a.id = b.id");

      String expected = "id,id\n" +
          "-------------------------------\n";

      assertEquals(expected, resultSetToString(res));
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$TEST_MIN_TASK_NUM.varname,
          ConfVars.$TEST_MIN_TASK_NUM.defaultVal);
      executeString("DROP TABLE table11 PURGE");
      executeString("DROP TABLE table12 PURGE");
    }
  }
  
  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testFullOuterJoinWithEmptyTable1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithEmptyTable1() throws Exception {
    runSimpleTests();
  }

  @Test
  public void testJoinOnMultipleDatabases() throws Exception {
    executeString("CREATE DATABASE JOINS");
    assertDatabaseExists("joins");
    executeString("CREATE TABLE JOINS.part_ as SELECT * FROM part");
    assertTableExists("joins.part_");
    executeString("CREATE TABLE JOINS.supplier_ as SELECT * FROM supplier");
    assertTableExists("joins.supplier_");
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE JOINS.part_ PURGE");
    executeString("DROP TABLE JOINS.supplier_ PURGE");
    executeString("DROP DATABASE JOINS");
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinWithJson() throws Exception {
    // select length(r_comment) as len, *, c_custkey*10 from customer, region order by len,r_regionkey,r_name
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinWithJson2() throws Exception {
    /*
    select t.n_nationkey, t.n_name, t.n_regionkey, t.n_comment, ps.ps_availqty, s.s_suppkey
    from (
      select n_nationkey, n_name, n_regionkey, n_comment
      from nation n
      join region r on (n.n_regionkey = r.r_regionkey)
    ) t
    join supplier s on (s.s_nationkey = t.n_nationkey)
    join partsupp ps on (s.s_suppkey = ps.ps_suppkey)
    where t.n_name in ('ARGENTINA','ETHIOPIA', 'MOROCCO');
     */
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testJoinOnMultipleDatabasesWithJson() throws Exception {
    executeString("CREATE DATABASE JOINS");
    assertDatabaseExists("joins");
    executeString("CREATE TABLE JOINS.part_ as SELECT * FROM part");
    assertTableExists("joins.part_");
    executeString("CREATE TABLE JOINS.supplier_ as SELECT * FROM supplier");
    assertTableExists("joins.supplier_");
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE JOINS.part_ PURGE");
    executeString("DROP TABLE JOINS.supplier_ PURGE");
    executeString("DROP DATABASE JOINS");
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinAsterisk() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithNull1() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithNull2() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoinWithNull3() throws Exception {
    runSimpleTests();
  }

  @Test
  public void testLeftOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id and t2.id = t3.id");

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,1,null\n" +
              "2,table11-2,null,null\n" +
              "3,table11-3,null,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testLeftOuterJoinPredicationCaseByCase2() throws Exception {
    // outer -> outer -> inner
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t2.id = t3.id\n" +
              "inner join table14 t4\n" +
              "on t2.id = t4.id"
      );

      String expected =
          "id,name,id,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,1,null,1\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testLeftOuterJoinPredicationCaseByCase2_1() throws Exception {
    // inner(on predication) -> outer(on predication) -> outer -> where
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "inner join table14 t4\n" +
              "on t1.id = t4.id and t4.id > 1\n" +
              "left outer join table13 t3\n" +
              "on t4.id = t3.id and t3.id = 2\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id \n" +
              "where t1.id > 1"
      );

      String expected =
          "id,name,id,id,id\n" +
              "-------------------------------\n" +
              "2,table11-2,null,2,2\n" +
              "3,table11-3,null,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testLeftOuterJoinPredicationCaseByCase3() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case J1: Join Predicate on Preserved Row Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id "
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,1,null\n" +
              "2,table11-2,null,2\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testLeftOuterJoinPredicationCaseByCase4() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case J2: Join Predicate on Null Supplying Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id and t2.id > 1 \n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,null,null\n" +
              "2,table11-2,null,2\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testLeftOuterJoinPredicationCaseByCase5() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W1: Where Predicate on Preserved Row Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "where t1.name > 'table11-1'"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "2,table11-2,null,2\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testLeftOuterJoinPredicationCaseByCase6() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W2: Where Predicate on Null Supplying Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "where t3.id > 2"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testLeftOuterWithEmptyTable() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W2: Where Predicate on Null Supplying Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id\n" +
              "from table11 t1\n" +
              "left outer join table15 t2\n" +
              "on t1.id = t2.id"
      );

      String expected =
          "id,name,id\n" +
              "-------------------------------\n" +
              "1,table11-1,null\n" +
              "2,table11-2,null\n" +
              "3,table11-3,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testRightOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id and t2.id = t3.id"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "null,null,null,2\n" +
              "null,null,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testRightOuterJoinPredicationCaseByCase2() throws Exception {
    // inner -> right
    // Notice: Join order should be preserved with origin order.
    // JoinEdge: t1 -> t4, t3 -> t1,t4
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "inner join table14 t4\n" +
              "on t1.id = t4.id and t4.id > 1\n" +
              "right outer join table13 t3\n" +
              "on t4.id = t3.id and t3.id = 2\n" +
              "where t3.id > 1"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "2,table11-2,2,2\n" +
              "null,null,3,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testRightOuterJoinPredicationCaseByCase3() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id "
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "null,null,null,2\n" +
              "null,null,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public void testFullOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();

    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "full outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "full outer join table14 t4\n" +
              "on t3.id = t4.id \n" +
              "order by t4.id"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "null,null,null,1\n" +
              "2,table11-2,2,2\n" +
              "3,table11-3,3,3\n" +
              "null,null,null,4\n" +
              "1,table11-1,null,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  private void createOuterJoinTestTable() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    data = new String[]{ "1|table12-1" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    data = new String[]{"2|table13-2", "3|table13-3" };
    TajoTestingCluster.createTable("table13", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    data = new String[]{"1|table14-1", "2|table14-2", "3|table14-3", "4|table14-4" };
    TajoTestingCluster.createTable("table14", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    data = new String[]{};
    TajoTestingCluster.createTable("table15", schema, tableOptions, data);
  }

  private void dropOuterJoinTestTable() throws Exception {
    executeString("DROP TABLE table11 PURGE;");
    executeString("DROP TABLE table12 PURGE;");
    executeString("DROP TABLE table13 PURGE;");
    executeString("DROP TABLE table14 PURGE;");
    executeString("DROP TABLE table15 PURGE;");
  }

  @Test
  public void testDifferentTypesJoinCondition() throws Exception {
    // select * from table20 t3 join table21 t4 on t3.id = t4.id;
    executeDDL("table1_int8_ddl.sql", "table1", "table20");
    executeDDL("table1_int4_ddl.sql", "table1", "table21");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table20");
      executeString("DROP TABLE table21");
    }
  }

  @Test
  @SimpleTest
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  public void testComplexJoinCondition1() throws Exception {
    // select n1.n_nationkey, n1.n_name, n2.n_name  from nation n1 join nation n2 on n1.n_name = upper(n2.n_name);
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition2() throws Exception {
    // select n1.n_nationkey, n1.n_name, upper(n2.n_name) name from nation n1 join nation n2
    // on n1.n_name = upper(n2.n_name);

    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition3() throws Exception {
    // select n1.n_nationkey, n1.n_name, n2.n_name from nation n1 join nation n2 on lower(n1.n_name) = lower(n2.n_name);
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition4() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition5() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition6() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testComplexJoinCondition7() throws Exception {
    runSimpleTests();
  }

  @Test
  public void testFullOuterJoinWithEmptyIntermediateData() throws Exception {
    ResultSet res = executeString(
        "select a.l_orderkey \n" +
            "from (select * from lineitem where l_orderkey < 0) a\n" +
            "full outer join (select * from lineitem where l_orderkey < 0) b\n" +
            "on a.l_orderkey = b.l_orderkey"
    );

    try {
      String expected =
          "l_orderkey\n" +
              "-------------------------------\n";

      assertEquals(expected, resultSetToString(res));
    } finally {
      cleanupQuery(res);
    }
  }

  @Test
  public void testJoinWithDifferentShuffleKey() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);

    List<String> data = new ArrayList<String>();

    int bytes = 0;
    for (int i = 0; i < 1000000; i++) {
      String row = i + "|" + i + "name012345678901234567890123456789012345678901234567890";
      bytes += row.getBytes().length;
      data.add(row);
      if (bytes > 2 * 1024 * 1024) {
        break;
      }
    }
    TajoTestingCluster.createTable("large_table", schema, tableOptions, data.toArray(new String[]{}));

    int originConfValue = conf.getIntVar(ConfVars.$DIST_QUERY_JOIN_PARTITION_VOLUME);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_JOIN_PARTITION_VOLUME.varname, "1");
    ResultSet res = executeString(
       "select count(b.id) " +
           "from (select id, count(*) as cnt from large_table group by id) a " +
           "left outer join (select id, count(*) as cnt from large_table where id < 200 group by id) b " +
           "on a.id = b.id"
    );

    try {
      String expected =
          "?count\n" +
              "-------------------------------\n" +
              "200\n";

      assertEquals(expected, resultSetToString(res));
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.$DIST_QUERY_JOIN_PARTITION_VOLUME.varname, "" + originConfValue);
      cleanupQuery(res);
      executeString("DROP TABLE large_table PURGE").close();
    }
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinFilterOfRowPreservedTable1() throws Exception {
    // this test is for join filter of a row preserved table.
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testJoinWithOrPredicates() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testNaturalJoin() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoin2() throws Exception {
    // large, large, small, small
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testLeftOuterJoin3() throws Exception {
    // large, large, small, large, small, small
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinAndCaseWhen() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithAsterisk1() throws Exception {
    // select region.*, customer.* from region, customer;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithAsterisk2() throws Exception {
    // select region.*, customer.* from customer, region;
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithAsterisk3() throws Exception {
    // select * from customer, region
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testCrossJoinWithAsterisk4() throws Exception {
    // select length(r_regionkey), *, c_custkey*10 from customer, region
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testBroadcastBasicJoin() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testBroadcastTwoPartJoin() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testBroadcastSubquery() throws Exception {
    runSimpleTests();
  }

  @Test
  @Option(withExplain = true, withExplainGlobal = true, parameterized = true)
  @SimpleTest()
  public void testBroadcastSubquery2() throws Exception {
    runSimpleTests();
  }

  @Test
  public void testBroadcastPartitionTable() throws Exception {
    // If all tables participate in the BROADCAST JOIN, there is some missing data.
    executeDDL("customer_partition_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer_partition.sql");
    res.close();

    createMultiFile("nation", 2, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new TextDatum(columnDatas[1]),
            new Int4Datum(Integer.parseInt(columnDatas[2])),
            new TextDatum(columnDatas[3])
        });
      }
    });

    createMultiFile("orders", 1, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new Int4Datum(Integer.parseInt(columnDatas[1])),
            new TextDatum(columnDatas[2])
        });
      }
    });

    res = executeQuery();
    assertResultSet(res);
    res.close();

    executeString("DROP TABLE customer_broad_parts PURGE");
    executeString("DROP TABLE nation_multifile PURGE");
    executeString("DROP TABLE orders_multifile PURGE");
  }

  @Test
  public void testBroadcastMultiColumnPartitionTable() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("testBroadcastMultiColumnPartitionTable");
    ResultSet res = testBase.execute(
        "create table " + tableName + " (col1 int4, col2 float4) partition by column(col3 text, col4 text) ");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName
        + " select o_orderkey, o_totalprice, substr(o_orderdate, 6, 2), substr(o_orderdate, 1, 4) from orders");
    res.close();

    res = executeString(
        "select distinct a.col3 from " + tableName + " as a " +
            "left outer join lineitem_large b " +
            "on a.col1 = b.l_orderkey order by a.col3"
    );

    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testCasebyCase1() throws Exception {
    // Left outer join with a small table and a large partition table which not matched any partition path.
    String tableName = CatalogUtil.normalizeIdentifier("largePartitionedTable");
    testBase.execute(
        "create table " + tableName + " (l_partkey int4, l_suppkey int4, l_linenumber int4, \n" +
            "l_quantity float8, l_extendedprice float8, l_discount float8, l_tax float8, \n" +
            "l_returnflag text, l_linestatus text, l_shipdate text, l_commitdate text, \n" +
            "l_receiptdate text, l_shipinstruct text, l_shipmode text, l_comment text) \n" +
            "partition by column(l_orderkey int4) ").close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    executeString("insert overwrite into " + tableName +
        " select l_partkey, l_suppkey, l_linenumber, \n" +
        " l_quantity, l_extendedprice, l_discount, l_tax, \n" +
        " l_returnflag, l_linestatus, l_shipdate, l_commitdate, \n" +
        " l_receiptdate, l_shipinstruct, l_shipmode, l_comment, l_orderkey from lineitem_large");

    ResultSet res = executeString(
        "select a.l_orderkey as key1, b.l_orderkey as key2 from lineitem as a " +
            "left outer join " + tableName + " b " +
            "on a.l_partkey = b.l_partkey and b.l_orderkey = 1000"
    );

    String expected = "key1,key2\n" +
        "-------------------------------\n" +
        "1,null\n" +
        "1,null\n" +
        "2,null\n" +
        "3,null\n" +
        "3,null\n";

    try {
      assertEquals(expected, resultSetToString(res));
    } finally {
      cleanupQuery(res);
    }
  }

  @Test
  public void testInnerAndOuterWithEmpty() throws Exception {
    executeDDL("customer_partition_ddl.sql", null);
    executeFile("insert_into_customer_partition.sql").close();

    // outer join table is empty
    ResultSet res = executeString(
        "select a.l_orderkey, b.o_orderkey, c.c_custkey from lineitem a " +
            "inner join orders b on a.l_orderkey = b.o_orderkey " +
            "left outer join customer_broad_parts c on a.l_orderkey = c.c_custkey and c.c_custkey < 0"
    );

    String expected = "l_orderkey,o_orderkey,c_custkey\n" +
        "-------------------------------\n" +
        "1,1,null\n" +
        "1,1,null\n" +
        "2,2,null\n" +
        "3,3,null\n" +
        "3,3,null\n";

    assertEquals(expected, resultSetToString(res));
    res.close();

    executeString("DROP TABLE customer_broad_parts PURGE").close();
  }

  interface TupleCreator {
    Tuple createTuple(String[] columnDatas);
  }

  private void createMultiFile(String tableName, int numRowsEachFile, TupleCreator tupleCreator) throws Exception {
    // make multiple small file
    String multiTableName = tableName + "_multifile";
    executeDDL(multiTableName + "_ddl.sql", null);

    TableDesc table = client.getTableDesc(multiTableName);
    assertNotNull(table);

    TableMeta tableMeta = table.getMeta();
    Schema schema = table.getLogicalSchema();

    File file = new File("src/test/tpch/" + tableName + ".tbl");

    if (!file.exists()) {
      file = new File(System.getProperty("user.dir") + "/tajo-core/src/test/tpch/" + tableName + ".tbl");
    }
    String[] rows = FileUtil.readTextFile(file).split("\n");

    assertTrue(rows.length > 0);

    int fileIndex = 0;

    Appender appender = null;
    for (int i = 0; i < rows.length; i++) {
      if (i % numRowsEachFile == 0) {
        if (appender != null) {
          appender.flush();
          appender.close();
        }
        Path dataPath = new Path(table.getPath().toString(), fileIndex + ".csv");
        fileIndex++;
        appender = ((FileStorageManager)StorageManager.getFileStorageManager(conf))
            .getAppender(tableMeta, schema, dataPath);
        appender.init();
      }
      String[] columnDatas = rows[i].split("\\|");
      Tuple tuple = tupleCreator.createTuple(columnDatas);
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
  }

  @Test
  public void testLeftOuterJoinLeftSideSmallTable() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.set(StorageConstants.TEXT_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1000000|a", "1000001|b", "2|c", "3|d", "4|e" };
    TajoTestingCluster.createTable("table1", schema, tableOptions, data, 1);

    data = new String[10000];
    for (int i = 0; i < data.length; i++) {
      data[i] = i + "|" + "this is testLeftOuterJoinLeftSideSmallTabletestLeftOuterJoinLeftSideSmallTable" + i;
    }
    TajoTestingCluster.createTable("table_large", schema, tableOptions, data, 2);

    try {
      ResultSet res = executeString(
          "select a.id, b.name from table1 a left outer join table_large b on a.id = b.id order by a.id"
      );

      String expected = "id,name\n" +
          "-------------------------------\n" +
          "2,this is testLeftOuterJoinLeftSideSmallTabletestLeftOuterJoinLeftSideSmallTable2\n" +
          "3,this is testLeftOuterJoinLeftSideSmallTabletestLeftOuterJoinLeftSideSmallTable3\n" +
          "4,this is testLeftOuterJoinLeftSideSmallTabletestLeftOuterJoinLeftSideSmallTable4\n" +
          "1000000,null\n" +
          "1000001,null\n";

      assertEquals(expected, resultSetToString(res));

      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table1 PURGE").close();
      executeString("DROP TABLE table_large PURGE").close();
    }
  }


  @Test
  public void testSelfJoin() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("paritioned_nation");
    ResultSet res = executeString(
        "create table " + tableName + " (n_name text,"
            + "  n_comment text, n_regionkey int8) USING csv "
            + "WITH ('csvfile.delimiter'='|')"
            + "PARTITION BY column(n_nationkey int8)");
    res.close();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName
            + " select n_name, n_comment, n_regionkey, n_nationkey from nation");
    res.close();

    res = executeString(
        "select a.n_nationkey, a.n_name from nation a join nation b on a.n_nationkey = b.n_nationkey"
            + " where a.n_nationkey in (1)");
    String expected = resultSetToString(res);
    res.close();

    res = executeString(
        "select a.n_nationkey, a.n_name from " + tableName + " a join "+tableName +
            " b on a.n_nationkey = b.n_nationkey "
            + " where a.n_nationkey in (1)");
    String resultSetData = resultSetToString(res);
    res.close();

    assertEquals(expected, resultSetData);

  }

  @Test
  public void testSelfJoin2() throws Exception {
    /*
     https://issues.apache.org/jira/browse/TAJO-1102
     See the following case.
     CREATE TABLE orders_partition
       (o_orderkey INT8, o_custkey INT8, o_totalprice FLOAT8, o_orderpriority TEXT,
          o_clerk TEXT, o_shippriority INT4, o_comment TEXT) USING CSV WITH ('csvfile.delimiter'='|')
       PARTITION BY COLUMN(o_orderdate TEXT, o_orderstatus TEXT);

     select a.o_orderstatus, count(*) as cnt
      from orders_partition a
      inner join orders_partition b
        on a.o_orderdate = b.o_orderdate
            and a.o_orderstatus = b.o_orderstatus
            and a.o_orderkey = b.o_orderkey
      where a.o_orderdate='1995-02-21'
        and a.o_orderstatus in ('F')
      group by a.o_orderstatus;

      Because of the where condition[where a.o_orderdate='1995-02-21 and a.o_orderstatus in ('F')],
        orders_partition table aliased a is small and broadcast target.
    */
    String tableName = CatalogUtil.normalizeIdentifier("partitioned_orders_large");
    ResultSet res = executeString(
        "create table " + tableName + " (o_orderkey INT8, o_custkey INT8, o_totalprice FLOAT8, o_orderpriority TEXT,\n" +
            "o_clerk TEXT, o_shippriority INT4, o_comment TEXT) USING CSV WITH ('csvfile.delimiter'='|')\n" +
            "PARTITION BY COLUMN(o_orderdate TEXT, o_orderstatus TEXT, o_orderkey_mod INT8)");
    res.close();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString(
        "insert overwrite into " + tableName +
            " select o_orderkey, o_custkey, o_totalprice, " +
            " o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderdate, o_orderstatus, o_orderkey % 10 " +
            " from orders_large ");
    res.close();

    res = executeString(
        "select a.o_orderdate, a.o_orderstatus, a.o_orderkey % 10 as o_orderkey_mod, a.o_totalprice " +
            "from orders_large a " +
            "join orders_large b on a.o_orderkey = b.o_orderkey " +
            "where a.o_orderdate = '1993-10-14' and a.o_orderstatus = 'F' and a.o_orderkey % 10 = 1" +
            " order by a.o_orderkey"
    );
    String expected = resultSetToString(res);
    res.close();

    res = executeString(
        "select a.o_orderdate, a.o_orderstatus, a.o_orderkey_mod, a.o_totalprice " +
            "from " + tableName +
            " a join "+ tableName + " b on a.o_orderkey = b.o_orderkey " +
            "where a.o_orderdate = '1993-10-14' and a.o_orderstatus = 'F' and a.o_orderkey_mod = 1 " +
            " order by a.o_orderkey"
    );
    String resultSetData = resultSetToString(res);
    res.close();

    assertEquals(expected, resultSetData);

  }
  @Test
  public void testMultipleBroadcastDataFileWithZeroLength() throws Exception {
    // According to node type(leaf or non-leaf) Broadcast join is determined differently by Repartitioner.
    // testMultipleBroadcastDataFileWithZeroLength testcase is for the leaf node
    createMultiFile("nation", 2, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new TextDatum(columnDatas[1]),
            new Int4Datum(Integer.parseInt(columnDatas[2])),
            new TextDatum(columnDatas[3])
        });
      }
    });
    addEmptyDataFile("nation_multifile", false);

    ResultSet res = executeQuery();

    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE nation_multifile PURGE");
  }

  @Test
  public void testMultipleBroadcastDataFileWithZeroLength2() throws Exception {
    // According to node type(leaf or non-leaf) Broadcast join is determined differently by Repartitioner.
    // testMultipleBroadcastDataFileWithZeroLength2 testcase is for the non-leaf node
    createMultiFile("nation", 2, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new TextDatum(columnDatas[1]),
            new Int4Datum(Integer.parseInt(columnDatas[2])),
            new TextDatum(columnDatas[3])
        });
      }
    });
    addEmptyDataFile("nation_multifile", false);

    ResultSet res = executeQuery();

    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE nation_multifile PURGE");
  }

  @Test
  public void testMultiplePartitionedBroadcastDataFileWithZeroLength() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("nation_partitioned");
    ResultSet res = testBase.execute(
        "create table " + tableName + " (n_name text) partition by column(n_nationkey int4, n_regionkey int4) ");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName
        + " select n_name, n_nationkey, n_regionkey from nation");
    res.close();

    addEmptyDataFile("nation_partitioned", true);

    res = executeQuery();

    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE nation_partitioned PURGE");
  }

  @Test
  public void testMultiplePartitionedBroadcastDataFileWithZeroLength2() throws Exception {
    String tableName = CatalogUtil.normalizeIdentifier("nation_partitioned");
    ResultSet res = testBase.execute(
        "create table " + tableName + " (n_name text) partition by column(n_nationkey int4, n_regionkey int4) ");
    res.close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(DEFAULT_DATABASE_NAME, tableName));

    res = executeString("insert overwrite into " + tableName
        + " select n_name, n_nationkey, n_regionkey from nation");
    res.close();

    addEmptyDataFile("nation_partitioned", true);

    res = executeQuery();

    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE nation_partitioned PURGE");
  }

  private void addEmptyDataFile(String tableName, boolean isPartitioned) throws Exception {
    TableDesc table = client.getTableDesc(tableName);

    Path path = new Path(table.getPath());
    FileSystem fs = path.getFileSystem(conf);
    if (isPartitioned) {
      List<Path> partitionPathList = getPartitionPathList(fs, path);
      for (Path eachPath: partitionPathList) {
        Path dataPath = new Path(eachPath, 0 + "_empty.csv");
        OutputStream out = fs.create(dataPath);
        out.close();
      }
    } else {
      Path dataPath = new Path(path, 0 + "_empty.csv");
      OutputStream out = fs.create(dataPath);
      out.close();
    }
  }

  private List<Path> getPartitionPathList(FileSystem fs, Path path) throws Exception {
    FileStatus[] files = fs.listStatus(path);
    List<Path> paths = new ArrayList<Path>();
    if (files != null) {
      for (FileStatus eachFile: files) {
        if (eachFile.isFile()) {
          paths.add(path);
          return paths;
        } else {
          paths.addAll(getPartitionPathList(fs, eachFile.getPath()));
        }
      }
    }

    return paths;
  }
}
