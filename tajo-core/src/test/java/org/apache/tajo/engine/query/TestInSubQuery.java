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

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class TestInSubQuery extends QueryTestCaseBase {

  public TestInSubQuery(String joinOption) {
    super(TajoConstants.DEFAULT_DATABASE_NAME);

    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname,
        TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
        TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);

    if (joinOption.indexOf("NoBroadcast") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname, "false");
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname, "-1");
    }

    if (joinOption.indexOf("Hash") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
    }
    if (joinOption.indexOf("Sort") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname, String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
          String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(1));
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"Hash_NoBroadcast"},
        {"Sort_NoBroadcast"},
        {"Hash"},
        {"Sort"},
    });
  }

  @AfterClass
  public static void classTearDown() {
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.varname,
        TajoConf.ConfVars.$TEST_BROADCAST_JOIN_ENABLED.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
        TajoConf.ConfVars.$DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_HASH_JOIN_SIZE_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        TajoConf.ConfVars.$EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);
  }

  @Test
  public final void testInSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInSubQuery2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNestedInSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInSubQueryWithOtherConditions() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testMultipleInSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInSubQueryWithJoin() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInSubQueryWithTableSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNotInSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testMultipleNotInSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNestedNotInSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInAndNotInSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNestedInAndNotInSubQuery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testNestedInSubQuery2() throws Exception {
    // select c_name from customer
    // where c_nationkey in (
    //    select n_nationkey from nation where n_name like 'C%' and n_regionkey in (
    //    select count(*)-1 from region where r_regionkey > 0 and r_regionkey < 3))
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
