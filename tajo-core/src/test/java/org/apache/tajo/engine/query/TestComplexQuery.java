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

package org.apache.tajo.engine.query;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

@Category(IntegrationTest.class)
public class TestComplexQuery extends QueryTestCaseBase {

  public TestComplexQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testGroupbyAfterJoin() throws Exception {
    // select l_linenumber, sum(l_quantity) from lineitem, orders where l_orderkey = o_orderkey and o_totalprice > 1 group by l_linenumber
//    conf.setBoolVar(TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO, false);
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
