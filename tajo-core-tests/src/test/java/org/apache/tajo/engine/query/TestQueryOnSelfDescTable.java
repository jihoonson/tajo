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

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.exception.AmbiguousColumnException;
import org.apache.tajo.exception.TajoException;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;

public class TestQueryOnSelfDescTable extends QueryTestCaseBase {

  public TestQueryOnSelfDescTable() throws IOException, TajoException {
    super();

    executeString(String.format("create external table self_desc_table1 using json location '%s'",
        getDataSetFile("sample1")));

    executeString(String.format("create external table self_desc_table2 using json location '%s'",
        getDataSetFile("sample2")));

    executeString(String.format("create external table self_desc_table3 using json location '%s'",
        getDataSetFile("tweets")));
  }

  @After
  public void teardown() throws TajoException {
    executeString("drop table self_desc_table1");
    executeString("drop table self_desc_table2");
    executeString("drop table self_desc_table3");
  }

  @Test
  public final void testSelect() throws Exception {
    ResultSet res = executeString("select glossary.title, glossary.\"GlossDiv\".title, glossary.\"GlossDiv\".null_expected, glossary.\"GlossDiv\".\"GlossList\".\"GlossEntry\".\"SortAs\", glossary.\"GlossDiv\".\"GlossList\".\"GlossEntry\".\"Abbrev\" from self_desc_table2");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testSelect2() throws Exception {
    ResultSet res = executeString("select glossary.title, glossary.\"GlossDiv\".title, glossary.\"GlossDiv\".null_expected, glossary.\"GlossDiv\".\"GlossList\".\"GlossEntry\".\"SortAs\" from self_desc_table2 where glossary.\"GlossDiv\".\"GlossList\".\"GlossEntry\".\"Abbrev\" = 'ISO 8879:1986'");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testGroupby() throws Exception {
    ResultSet res = executeString("select name.first_name, count(*) from self_desc_table1 group by name.first_name");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testGroupby2() throws Exception {
    ResultSet res = executeString("select coordinates, avg(retweet_count::int4) from self_desc_table3 group by coordinates");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testGroupby3() throws Exception {
    ResultSet res = executeString("select user.time_zone, sum(user.favourites_count::int8) from self_desc_table3 group by user.time_zone");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testSort() throws Exception {
    ResultSet res = executeString("select created_at, id, user.profile_sidebar_fill_color from self_desc_table3 order by created_at");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testCrossJoin() throws Exception {
    ResultSet res = executeString("select user.favourites_count::int8, l_linenumber, l_comment from default.lineitem, self_desc_table3");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testJoinWithSchemaFullTable() throws Exception {
    ResultSet res = executeString("select user.favourites_count::int8, l_linenumber, l_comment from default.lineitem, self_desc_table3 where user.favourites_count::int8 = (l_orderkey - 1)");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testJoinWithSchemaFullTable2() throws Exception {
    ResultSet res = executeString("select user.favourites_count::int8, l_linenumber, l_comment from default.lineitem, self_desc_table3, default.orders, default.supplier where user.favourites_count::int8 = (l_orderkey - 1) and l_orderkey = o_orderkey and l_linenumber = s_suppkey");
    System.out.println(resultSetToString(res));
  }

  @Test(expected = AmbiguousColumnException.class)
  public final void testJoinWithSchemaFullTable3() throws Exception {
    ResultSet res = executeString("select user.favourites_count::int8, l_linenumber, l_comment from default.lineitem, self_desc_table1, self_desc_table3, default.orders, default.supplier where user.favourites_count::int8 = (l_orderkey - 1) and l_orderkey = o_orderkey and l_linenumber = s_suppkey and self_desc_table3.user.favourites_count = self_desc_table1.name.first_name");
    System.out.println(resultSetToString(res));
  }

  @Test
  public final void testJoinWithSchemaFullTable4() throws Exception {
    ResultSet res = executeString("select self_desc_table3.user.favourites_count::int8, l_linenumber, l_comment from default.lineitem, self_desc_table1, self_desc_table3, default.orders, default.supplier where self_desc_table3.user.favourites_count::int8 = (l_orderkey - 1) and l_orderkey = o_orderkey and l_linenumber = s_suppkey and self_desc_table3.user.favourites_count = self_desc_table1.name.first_name");
    System.out.println(resultSetToString(res));
  }

  @Test(expected = AmbiguousColumnException.class)
  public final void testJoinOfSelfDescTables() throws Exception {
    ResultSet res = executeString("select user.favourites_count::int8 from self_desc_table1, self_desc_table3 where user.favourites_count = name.first_name");
  }

  @Test
  public final void testJoinOfSelfDescTablesWithQualifiedColumns() throws Exception {
    ResultSet res = executeString("select self_desc_table3.user.favourites_count::int8 from self_desc_table1, self_desc_table3 where self_desc_table3.user.favourites_count = self_desc_table1.name.first_name");
    System.out.println(resultSetToString(res));
  }

  @Test(expected = AmbiguousColumnException.class)
  public final void testJoinWithSingleQualifiedColumn() throws Exception {
    ResultSet res = executeString("select self_desc_table3.user.favourites_count::int8 from self_desc_table1, self_desc_table3 where self_desc_table3.user.favourites_count = name.first_name");
  }
}
