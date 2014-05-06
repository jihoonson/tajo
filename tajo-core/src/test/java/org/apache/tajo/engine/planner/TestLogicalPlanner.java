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

package org.apache.tajo.engine.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.function.builtin.SumInt;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.TUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestLogicalPlanner {
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
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234");
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);

    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }

    Schema schema = new Schema();
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("empid", Type.INT4);
    schema.addColumn("deptname", Type.TEXT);

    Schema schema2 = new Schema();
    schema2.addColumn("deptname", Type.TEXT);
    schema2.addColumn("manager", Type.TEXT);

    Schema schema3 = new Schema();
    schema3.addColumn("deptname", Type.TEXT);
    schema3.addColumn("score", Type.INT4);

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
    TableDesc people = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "employee"), schema, meta,
        CommonTestingUtil.getTestDir());
    catalog.createTable(people);

    TableDesc student = new TableDesc(
        CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), schema2, StoreType.CSV, new Options(),
        CommonTestingUtil.getTestDir());
    catalog.createTable(student);

    TableDesc score = new TableDesc(
        CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), schema3, StoreType.CSV, new Options(),
        CommonTestingUtil.getTestDir());
    catalog.createTable(score);

    FunctionDesc funcDesc = new FunctionDesc("sumtest", SumInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4));


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
          CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, table), tpch.getSchema(table), m,
          CommonTestingUtil.getTestDir());
      catalog.createTable(d);
    }

    catalog.createFunction(funcDesc);
    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  static String[] QUERIES = {
      "select name, empid, deptname from employee where empId > 500", // 0
      "select name, empid, e.deptname, manager from employee as e, dept as dp", // 1
      "select name, empid, e.deptname, manager, score from employee as e, dept, score", // 2
      "select p.deptname, sumtest(score) from dept as p, score group by p.deptName having sumtest(score) > 30", // 3
      "select p.deptname, score from dept as p, score order by score asc", // 4
      "select name from employee where empId = 100", // 5
      "select name, score from employee, score", // 6
      "select p.deptName, sumtest(score) from dept as p, score group by p.deptName", // 7
      "create table store1 as select p.deptName, sumtest(score) from dept as p, score group by p.deptName", // 8
      "select deptName, sumtest(score) from score group by deptName having sumtest(score) > 30", // 9
      "select 7 + 8 as res1, 8 * 9 as res2, 10 * 10 as res3", // 10
      "create index idx_employee on employee using bitmap (name null first, empId desc) with ('fillfactor' = 70)", // 11
      "select name, score from employee, score order by score limit 3", // 12
      "select length(name), length(deptname), *, empid+10 from employee where empId > 500", // 13
  };

  @Test
  public final void testSingleRelation() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[0]);
    LogicalPlan planNode = planner.createPlan(session, expr);
    LogicalNode plan = planNode.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    TestLogicalNode.testCloneLogicalNode(plan);
    LogicalRootNode root = (LogicalRootNode) plan;
    testJsonSerDerObject(planNode, root);

    assertEquals(NodeType.PROJECTION, planNode.getChild(root).getType());
    ProjectionNode projNode = planNode.getChild(root);

    assertEquals(NodeType.SELECTION, planNode.getChild(projNode).getType());
    SelectionNode selNode = planNode.getChild(projNode);

    assertEquals(NodeType.SCAN, planNode.getChild(selNode).getType());
    ScanNode scanNode = planNode.getChild(selNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), scanNode.getTableName());
  }

  public static void assertSchema(Schema expected, Schema schema) {
    Column expectedColumn;
    Column column;
    for (int i = 0; i < expected.size(); i++) {
      expectedColumn = expected.getColumn(i);
      column = schema.getColumn(expectedColumn.getSimpleName());
      assertEquals(expectedColumn.getSimpleName(), column.getSimpleName());
      assertEquals(expectedColumn.getDataType(), column.getDataType());
    }
  }

  @Test
  public final void testImplicityJoinPlan() throws CloneNotSupportedException, PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[1]);
    LogicalPlan planNode = planner.createPlan(session, expr);
    LogicalNode plan = planNode.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    testJsonSerDerObject(planNode, root);
    TestLogicalNode.testCloneLogicalNode(root);

    Schema expectedSchema = new Schema();
    expectedSchema.addColumn("name", Type.TEXT);
    expectedSchema.addColumn("empid", Type.INT4);
    expectedSchema.addColumn("deptname", Type.TEXT);
    expectedSchema.addColumn("manager", Type.TEXT);
    for (int i = 0; i < expectedSchema.size(); i++) {
      Column found = root.getOutSchema().getColumn(expectedSchema.getColumn(i).getSimpleName());
      assertEquals(expectedSchema.getColumn(i).getDataType(), found.getDataType());
    }

    assertEquals(NodeType.PROJECTION, planNode.getChild(root).getType());
    ProjectionNode projNode = planNode.getChild(root);

    assertEquals(NodeType.JOIN, planNode.getChild(projNode).getType());
    JoinNode joinNode = planNode.getChild(projNode);

    assertEquals(NodeType.SCAN, planNode.getLeftChild(joinNode).getType());
    ScanNode leftNode = planNode.getLeftChild(joinNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), leftNode.getTableName());
    assertEquals(NodeType.SCAN, planNode.getRightChild(joinNode).getType());
    ScanNode rightNode = planNode.getRightChild(joinNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), rightNode.getTableName());

    // three relations
    expr = sqlAnalyzer.parse(QUERIES[2]);
    plan = planner.createPlan(session, expr).getRootBlock().getRoot();
    testJsonSerDerObject(planNode, plan);
    TestLogicalNode.testCloneLogicalNode(plan);

    expectedSchema.addColumn("score", Type.INT4);
    assertSchema(expectedSchema, plan.getOutSchema());

    assertEquals(NodeType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;

    assertEquals(NodeType.PROJECTION, planNode.getChild(root).getType());
    projNode = planNode.getChild(root);

    assertEquals(NodeType.JOIN, planNode.getChild(projNode).getType());
    joinNode = planNode.getChild(projNode);

    assertEquals(NodeType.JOIN, planNode.getLeftChild(joinNode).getType());

    assertEquals(NodeType.SCAN, planNode.getRightChild(joinNode).getType());
    ScanNode scan1 = planNode.getRightChild(joinNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), scan1.getTableName());

    JoinNode leftNode2 = planNode.getLeftChild(joinNode);
    assertEquals(NodeType.JOIN, leftNode2.getType());

    assertEquals(NodeType.SCAN, planNode.getLeftChild(leftNode2).getType());
    ScanNode leftScan = planNode.getLeftChild(leftNode2);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), leftScan.getTableName());

    assertEquals(NodeType.SCAN, planNode.getRightChild(leftNode2).getType());
    ScanNode rightScan = planNode.getRightChild(leftNode2);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), rightScan.getTableName());
  }



  String [] JOINS = {
      "select name, dept.deptName, score from employee natural join dept natural join score", // 0
      "select name, dept.deptName, score from employee inner join dept on employee.deptName = dept.deptName inner join score on dept.deptName = score.deptName", // 1
      "select name, dept.deptName, score from employee left outer join dept on employee.deptName = dept.deptName right outer join score on dept.deptName = score.deptName" // 2
  };

  static Schema expectedJoinSchema;
  static {
    expectedJoinSchema = new Schema();
    expectedJoinSchema.addColumn("name", Type.TEXT);
    expectedJoinSchema.addColumn("deptname", Type.TEXT);
    expectedJoinSchema.addColumn("score", Type.INT4);
  }

  @Test
  public final void testNaturalJoinPlan() throws PlanningException {
    // two relations
    Expr context = sqlAnalyzer.parse(JOINS[0]);
    LogicalPlan planNode = planner.createPlan(session, context);
    LogicalNode plan = planNode.getRootBlock().getRoot();
    testJsonSerDerObject(planNode, plan);
    assertSchema(expectedJoinSchema, plan.getOutSchema());

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    assertEquals(NodeType.PROJECTION, planNode.getChild(root).getType());
    ProjectionNode proj = planNode.getChild(root);
    assertEquals(NodeType.JOIN, planNode.getChild(proj).getType());
    JoinNode join = planNode.getChild(proj);
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(NodeType.SCAN, planNode.getRightChild(join).getType());
    assertTrue(join.hasJoinQual());
    ScanNode scan = planNode.getRightChild(join);
    assertEquals("default.score", scan.getTableName());

    assertEquals(NodeType.JOIN, planNode.getLeftChild(join).getType());
    join = planNode.getLeftChild(join);
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(NodeType.SCAN, planNode.getLeftChild(join).getType());
    ScanNode outer = planNode.getLeftChild(join);
    assertEquals("default.employee", outer.getTableName());
    assertEquals(NodeType.SCAN, planNode.getRightChild(join).getType());
    ScanNode inner = planNode.getRightChild(join);
    assertEquals("default.dept", inner.getTableName());
  }

  @Test
  public final void testInnerJoinPlan() throws PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(JOINS[1]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    assertSchema(expectedJoinSchema, root.getOutSchema());

    assertEquals(NodeType.ROOT, root.getType());
    assertEquals(NodeType.PROJECTION, plan.getChild(root).getType());
    ProjectionNode proj = plan.getChild(root);
    assertEquals(NodeType.JOIN, plan.getChild(proj).getType());
    JoinNode join = plan.getChild(proj);
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(NodeType.SCAN, plan.getRightChild(join).getType());
    ScanNode scan = plan.getRightChild(join);
    assertEquals("default.score", scan.getTableName());

    assertEquals(NodeType.JOIN, plan.getLeftChild(join).getType());
    join = plan.getLeftChild(join);
    assertEquals(JoinType.INNER, join.getJoinType());
    assertEquals(NodeType.SCAN, plan.getLeftChild(join).getType());
    ScanNode outer = plan.getLeftChild(join);
    assertEquals("default.employee", outer.getTableName());
    assertEquals(NodeType.SCAN, plan.getRightChild(join).getType());
    ScanNode inner = plan.getRightChild(join);
    assertEquals("default.dept", inner.getTableName());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalType.EQUAL, join.getJoinQual().getType());
  }

  @Test
  public final void testOuterJoinPlan() throws PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(JOINS[2]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    assertSchema(expectedJoinSchema, root.getOutSchema());

    assertEquals(NodeType.ROOT, root.getType());
    assertEquals(NodeType.PROJECTION,  plan.getChild(root).getType());
    ProjectionNode proj = plan.getChild(root);
    assertEquals(NodeType.JOIN, plan.getChild(proj).getType());
    JoinNode join = plan.getChild(proj);
    assertEquals(JoinType.RIGHT_OUTER, join.getJoinType());
    assertEquals(NodeType.SCAN, plan.getRightChild(join).getType());
    ScanNode scan = plan.getRightChild(join);
    assertEquals("default.score", scan.getTableName());

    assertEquals(NodeType.JOIN, plan.getLeftChild(join).getType());
    join = plan.getLeftChild(join);
    assertEquals(JoinType.LEFT_OUTER, join.getJoinType());
    assertEquals(NodeType.SCAN, plan.getLeftChild(join).getType());
    ScanNode outer = plan.getLeftChild(join);
    assertEquals("default.employee", outer.getTableName());
    assertEquals(NodeType.SCAN, plan.getRightChild(join).getType());
    ScanNode inner = plan.getRightChild(join);
    assertEquals("default.dept", inner.getTableName());
    assertTrue(join.hasJoinQual());
    assertEquals(EvalType.EQUAL, join.getJoinQual().getType());
  }


  @Test
  public final void testGroupby() throws CloneNotSupportedException, PlanningException {
    // without 'having clause'
    Expr context = sqlAnalyzer.parse(QUERIES[7]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode root = plan.getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, root.getType());
    testJsonSerDerObject(plan, root);
    testQuery7(plan, plan.getChild(root));

    // with having clause
    context = sqlAnalyzer.parse(QUERIES[3]);
    plan = planner.createPlan(session, context);
    root = plan.getRootBlock().getRoot();
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(NodeType.ROOT, root.getType());

    assertEquals(NodeType.PROJECTION, plan.getChild(root).getType());
    ProjectionNode projNode = plan.getChild(root);
    assertEquals(NodeType.HAVING, plan.getChild(projNode).getType());
    HavingNode havingNode = plan.getChild(projNode);
    assertEquals(NodeType.GROUP_BY, plan.getChild(havingNode).getType());
    GroupbyNode groupByNode = plan.getChild(havingNode);

    assertEquals(NodeType.JOIN, plan.getChild(groupByNode).getType());
    JoinNode joinNode = plan.getChild(groupByNode);

    assertEquals(NodeType.SCAN, plan.getLeftChild(joinNode).getType());
    ScanNode leftNode = plan.getLeftChild(joinNode);
    assertEquals("default.dept", leftNode.getTableName());
    assertEquals(NodeType.SCAN, plan.getRightChild(joinNode).getType());
    ScanNode rightNode = plan.getRightChild(joinNode);
    assertEquals("default.score", rightNode.getTableName());

    //LogicalOptimizer.optimize(context, plan);
  }


  @Test
  public final void testMultipleJoin() throws IOException, PlanningException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File("src/test/resources/queries/TestJoinQuery/testTPCHQ2Join.sql")));
    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(), expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    Schema expected = tpch.getOutSchema("q2");
    assertSchema(expected, root.getOutSchema());
  }

  private final void findJoinQual(EvalNode evalNode, Map<BinaryEval, Boolean> qualMap,
                                  EvalType leftType, EvalType rightType)
      throws IOException, PlanningException {
    Preconditions.checkArgument(evalNode instanceof BinaryEval);
    BinaryEval qual = (BinaryEval)evalNode;

    if (qual.getLeftExpr().getType() == leftType && qual.getRightExpr().getType() == rightType) {
      assertEquals(qual.getLeftExpr().getType(), EvalType.FIELD);
      FieldEval leftField = (FieldEval)qual.getLeftExpr();

      for (Map.Entry<BinaryEval, Boolean> entry : qualMap.entrySet()) {
        FieldEval leftJoinField = (FieldEval)entry.getKey().getLeftExpr();

        if (qual.getRightExpr().getType() == entry.getKey().getRightExpr().getType()) {
          if (rightType == EvalType.FIELD) {
            FieldEval rightField = (FieldEval)qual.getRightExpr();
            FieldEval rightJoinField = (FieldEval)entry.getKey().getRightExpr();

            if (leftJoinField.getColumnRef().getQualifiedName().equals(leftField.getColumnRef().getQualifiedName())
                && rightField.getColumnRef().getQualifiedName().equals(rightJoinField.getColumnRef().getQualifiedName())) {
              qualMap.put(entry.getKey(), Boolean.TRUE);
            }
          } else if (rightType == EvalType.CONST) {
            ConstEval rightField = (ConstEval)qual.getRightExpr();
            ConstEval rightJoinField = (ConstEval)entry.getKey().getRightExpr();

            if (leftJoinField.getColumnRef().getQualifiedName().equals(leftField.getColumnRef().getQualifiedName()) &&
                rightField.getValue().equals(rightJoinField.getValue())) {
              qualMap.put(entry.getKey(), Boolean.TRUE);
            }
          } else if (rightType == EvalType.ROW_CONSTANT) {
            RowConstantEval rightField = (RowConstantEval)qual.getRightExpr();
            RowConstantEval rightJoinField = (RowConstantEval)entry.getKey().getRightExpr();

            if (leftJoinField.getColumnRef().getQualifiedName().equals(leftField.getColumnRef().getQualifiedName())) {
              assertEquals(rightField.getValues().length, rightJoinField.getValues().length);
              for (int i = 0; i < rightField.getValues().length; i++) {
                assertEquals(rightField.getValues()[i], rightJoinField.getValues()[i]);
              }
              qualMap.put(entry.getKey(), Boolean.TRUE);
            }
          }
        }
      }
    }
  }

  @Test
  public final void testJoinWithMultipleJoinQual1() throws IOException, PlanningException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File
            ("src/test/resources/queries/TestJoinQuery/testJoinWithMultipleJoinQual1.sql")));

    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(),expr);
    LogicalNode node = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, node);

    Schema expected = tpch.getOutSchema("q2");
    assertSchema(expected, node.getOutSchema());

    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration());
    optimizer.optimize(plan);

    LogicalNode[] nodes = PlannerUtil.findAllNodes(plan.getLogicalNodeTree(), node, NodeType.JOIN);
    Map<BinaryEval, Boolean> qualMap = TUtil.newHashMap();
    BinaryEval joinQual = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_regionkey", Type.INT4))
        , new FieldEval(new Column("default.ps.ps_suppkey", Type.INT4))
        );
    qualMap.put(joinQual, Boolean.FALSE);

    for(LogicalNode eachNode : nodes) {
      JoinNode joinNode = (JoinNode)eachNode;
      EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual());

      for(EvalNode evalNode : evalNodes) {
        findJoinQual(evalNode, qualMap, EvalType.FIELD, EvalType.FIELD);
      }
    }

    for (Map.Entry<BinaryEval, Boolean> entry : qualMap.entrySet()) {
      if (!entry.getValue().booleanValue()) {
        Preconditions.checkArgument(false,
            "JoinQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }
  }

  @Test
  public final void testJoinWithMultipleJoinQual2() throws IOException, PlanningException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File
            ("src/test/resources/queries/TestJoinQuery/testJoinWithMultipleJoinQual2.sql")));

    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(),expr);
    LogicalNode node = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, node);

    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration());
    optimizer.optimize(plan);

    LogicalNode[] nodes = PlannerUtil.findAllNodes(plan.getLogicalNodeTree(), node, NodeType.SCAN);
    Map<BinaryEval, Boolean> qualMap = TUtil.newHashMap();
    BinaryEval joinQual = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_name", Type.TEXT))
        , new ConstEval(new TextDatum("MOROCCO"))
    );
    qualMap.put(joinQual, Boolean.FALSE);

    for(LogicalNode eachNode : nodes) {
      ScanNode scanNode = (ScanNode)eachNode;
      if (scanNode.hasQual()) {
        EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(scanNode.getQual());

        for(EvalNode evalNode : evalNodes) {
          findJoinQual(evalNode, qualMap, EvalType.FIELD, EvalType.CONST);
        }
      }
    }

    for (Map.Entry<BinaryEval, Boolean> entry : qualMap.entrySet()) {
      if (!entry.getValue().booleanValue()) {
        Preconditions.checkArgument(false,
            "SelectionQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }
  }

  @Test
  public final void testJoinWithMultipleJoinQual3() throws IOException, PlanningException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File
            ("src/test/resources/queries/TestJoinQuery/testJoinWithMultipleJoinQual3.sql")));

    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(),expr);
    LogicalNode node = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, node);

    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration());
    optimizer.optimize(plan);

    LogicalNode[] nodes = PlannerUtil.findAllNodes(plan.getLogicalNodeTree(), node, NodeType.SCAN);
    Map<BinaryEval, Boolean> qualMap = TUtil.newHashMap();
    TextDatum[] datums = new TextDatum[3];
    datums[0] = new TextDatum("ARGENTINA");
    datums[1] = new TextDatum("ETHIOPIA");
    datums[2] = new TextDatum("MOROCCO");

    BinaryEval joinQual = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_name", Type.TEXT))
        , new RowConstantEval(datums)
    );
    qualMap.put(joinQual, Boolean.FALSE);

    for(LogicalNode eachNode : nodes) {
      ScanNode scanNode = (ScanNode)eachNode;
      if (scanNode.hasQual()) {
        EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(scanNode.getQual());

        for(EvalNode evalNode : evalNodes) {
          findJoinQual(evalNode, qualMap, EvalType.FIELD, EvalType.ROW_CONSTANT);
        }
      }
    }

    for (Map.Entry<BinaryEval, Boolean> entry : qualMap.entrySet()) {
      if (!entry.getValue().booleanValue()) {
        Preconditions.checkArgument(false,
            "ScanQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }
  }


  @Test
  public final void testJoinWithMultipleJoinQual4() throws IOException, PlanningException {
    Expr expr = sqlAnalyzer.parse(
        FileUtil.readTextFile(new File
            ("src/test/resources/queries/TestJoinQuery/testJoinWithMultipleJoinQual4.sql")));

    LogicalPlan plan = planner.createPlan(LocalTajoTestingUtility.createDummySession(),expr);
    LogicalNode node = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, node);

    LogicalOptimizer optimizer = new LogicalOptimizer(util.getConfiguration());
    optimizer.optimize(plan);

    Map<BinaryEval, Boolean> scanMap = TUtil.newHashMap();
    TextDatum[] datums = new TextDatum[3];
    datums[0] = new TextDatum("ARGENTINA");
    datums[1] = new TextDatum("ETHIOPIA");
    datums[2] = new TextDatum("MOROCCO");

    BinaryEval scanQual = new BinaryEval(EvalType.EQUAL
        , new FieldEval(new Column("default.n.n_name", Type.TEXT))
        , new RowConstantEval(datums)
    );
    scanMap.put(scanQual, Boolean.FALSE);

    Map<BinaryEval, Boolean> joinQualMap = TUtil.newHashMap();
    BinaryEval joinQual = new BinaryEval(EvalType.GTH
        , new FieldEval(new Column("default.t.n_nationkey", Type.INT4))
        , new FieldEval(new Column("default.s.s_suppkey", Type.INT4))
    );
    joinQualMap.put(joinQual, Boolean.FALSE);

    LogicalNode[] nodes = PlannerUtil.findAllNodes(plan.getLogicalNodeTree(), node, NodeType.JOIN);
    for(LogicalNode eachNode : nodes) {
      JoinNode joinNode = (JoinNode)eachNode;
      if (joinNode.hasJoinQual()) {
        EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual());

        for(EvalNode evalNode : evalNodes) {
          findJoinQual(evalNode, joinQualMap, EvalType.FIELD, EvalType.FIELD);
        }
      }
    }

    nodes = PlannerUtil.findAllNodes(plan.getLogicalNodeTree(), node, NodeType.SCAN);
    for(LogicalNode eachNode : nodes) {
      ScanNode scanNode = (ScanNode)eachNode;
      if (scanNode.hasQual()) {
        EvalNode[] evalNodes = AlgebraicUtil.toConjunctiveNormalFormArray(scanNode.getQual());

        for(EvalNode evalNode : evalNodes) {
          findJoinQual(evalNode, scanMap, EvalType.FIELD, EvalType.ROW_CONSTANT);
        }
      }
    }


    for (Map.Entry<BinaryEval, Boolean> entry : joinQualMap.entrySet()) {
      if (!entry.getValue().booleanValue()) {
        Preconditions.checkArgument(false,
            "JoinQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }

    for (Map.Entry<BinaryEval, Boolean> entry : scanMap.entrySet()) {
      if (!entry.getValue().booleanValue()) {
        Preconditions.checkArgument(false,
            "ScanQual not found. -> required JoinQual:" + entry.getKey().toJson());
      }
    }
  }

  static void testQuery7(LogicalPlan plan, LogicalNode root) {
    assertEquals(NodeType.PROJECTION, root.getType());
    ProjectionNode projNode = (ProjectionNode) root;
    assertEquals(NodeType.GROUP_BY, plan.getChild(projNode).getType());
    GroupbyNode groupByNode = plan.getChild(projNode);

    assertEquals(NodeType.JOIN, plan.getChild(groupByNode).getType());
    JoinNode joinNode = plan.getChild(groupByNode);

    assertEquals(NodeType.SCAN, plan.getLeftChild(joinNode).getType());
    ScanNode leftNode = plan.getLeftChild(joinNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), leftNode.getTableName());
    assertEquals(NodeType.SCAN, plan.getRightChild(joinNode).getType());
    ScanNode rightNode = plan.getRightChild(joinNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), rightNode.getTableName());
  }


  @Test
  public final void testStoreTable() throws CloneNotSupportedException, PlanningException {
    Expr context = sqlAnalyzer.parse(QUERIES[8]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode root = plan.getRootBlock().getRoot();
    TestLogicalNode.testCloneLogicalNode(root);
    testJsonSerDerObject(plan, root);

    assertEquals(NodeType.ROOT, root.getType());

    assertEquals(NodeType.CREATE_TABLE, plan.getChild(root).getType());
    StoreTableNode storeNode = plan.getChild(root);
    testQuery7(plan, plan.getChild(storeNode));
  }

  @Test
  public final void testOrderBy() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[4]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(NodeType.ROOT, root.getType());

    assertEquals(NodeType.PROJECTION, plan.getChild(root).getType());
    ProjectionNode projNode = plan.getChild(root);

    assertEquals(NodeType.SORT, plan.getChild(projNode).getType());
    SortNode sortNode = plan.getChild(projNode);

    assertEquals(NodeType.JOIN, plan.getChild(sortNode).getType());
    JoinNode joinNode = plan.getChild(sortNode);

    assertEquals(NodeType.SCAN, plan.getLeftChild(joinNode).getType());
    ScanNode leftNode = plan.getLeftChild(joinNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "dept"), leftNode.getTableName());
    assertEquals(NodeType.SCAN, plan.getRightChild(joinNode).getType());
    ScanNode rightNode = plan.getRightChild(joinNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), rightNode.getTableName());
  }

  @Test
  public final void testLimit() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[12]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(NodeType.ROOT, root.getType());

    assertEquals(NodeType.PROJECTION, plan.getChild(root).getType());
    ProjectionNode projNode = plan.getChild(root);

    assertEquals(NodeType.LIMIT, plan.getChild(projNode).getType());
    LimitNode limitNode = plan.getChild(projNode);

    assertEquals(NodeType.SORT, plan.getChild(limitNode).getType());
  }

  @Test
  public final void testSPJPush() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[5]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(NodeType.ROOT, root.getType());
    assertEquals(NodeType.PROJECTION, plan.getChild(root).getType());
    ProjectionNode projNode = plan.getChild(root);
    assertEquals(NodeType.SELECTION, plan.getChild(projNode).getType());
    SelectionNode selNode = plan.getChild(projNode);
    assertEquals(NodeType.SCAN, plan.getChild(selNode).getType());
    ScanNode scanNode = plan.getChild(selNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), scanNode.getTableName());
  }



  @Test
  public final void testSPJ() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[6]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    TestLogicalNode.testCloneLogicalNode(root);
  }

  @Test
  public final void testJson() throws PlanningException {
	  Expr expr = sqlAnalyzer.parse(QUERIES[9]);
    LogicalPlan plan = planner.createPlan(session, expr);
	  LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);

	  String json = plan.getLogicalNodeTree().toJson();
	  LogicalNodeTree fromJson = CoreGsonHelper.fromJson(json, LogicalNodeTree.class);
	  assertEquals(NodeType.ROOT, root.getType());
	  LogicalNode project = fromJson.getChild(root);
	  assertEquals(NodeType.PROJECTION, project.getType());
	  assertEquals(NodeType.HAVING, fromJson.getChild(project).getType());
    HavingNode havingNode = fromJson.getChild(project);
    assertEquals(NodeType.GROUP_BY, fromJson.getChild(havingNode).getType());
    GroupbyNode groupbyNode = fromJson.getChild(havingNode);
    assertEquals(NodeType.SCAN, fromJson.getChild(groupbyNode).getType());
	  LogicalNode scan = fromJson.getChild(groupbyNode);
	  assertEquals(NodeType.SCAN, scan.getType());
  }

  @Test
  public final void testVisitor() throws PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[1]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();

    TestVisitor vis = new TestVisitor();
    plan.getLogicalNodeTree().postOrder(vis, root);
//    plan.postOrder(vis);

    assertEquals(NodeType.ROOT, vis.stack.pop().getType());
    assertEquals(NodeType.PROJECTION, vis.stack.pop().getType());
    assertEquals(NodeType.JOIN, vis.stack.pop().getType());
    assertEquals(NodeType.SCAN, vis.stack.pop().getType());
    assertEquals(NodeType.SCAN, vis.stack.pop().getType());
  }

  private static class TestVisitor implements LogicalNodeVisitor {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    @Override
    public void visit(LogicalNode node) {
      stack.push(node);
    }
  }


  @Test
  public final void testExprNode() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[10]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    assertEquals(NodeType.ROOT, root.getType());
    assertEquals(NodeType.EXPRS, plan.getChild(root).getType());
    Schema out = root.getOutSchema();

    Iterator<Column> it = out.getColumns().iterator();
    Column col = it.next();
    assertEquals("res1", col.getSimpleName());
    col = it.next();
    assertEquals("res2", col.getSimpleName());
    col = it.next();
    assertEquals("res3", col.getSimpleName());
  }

  @Test
  public final void testAsterisk() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[13]);
    LogicalPlan planNode = planner.createPlan(session, expr);
    LogicalNode plan = planNode.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    TestLogicalNode.testCloneLogicalNode(plan);
    LogicalRootNode root = (LogicalRootNode) plan;
    testJsonSerDerObject(planNode, root);

    assertEquals(NodeType.PROJECTION, planNode.getChild(root).getType());
    ProjectionNode projNode = planNode.getChild(root);
    assertEquals(6, projNode.getOutSchema().size());

    assertEquals(NodeType.SELECTION, planNode.getChild(projNode).getType());
    SelectionNode selNode = planNode.getChild(projNode);

    assertEquals(NodeType.SCAN, planNode.getChild(selNode).getType());
    ScanNode scanNode = planNode.getChild(selNode);
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), scanNode.getTableName());
  }

  static final String ALIAS [] = {
    "select deptName, sum(score) as total from score group by deptName",
    "select em.empId as id, sum(score) as total from employee as em inner join score using (em.deptName) group by id"
  };


  @Test
  public final void testAlias1() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(ALIAS[0]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);

    Schema finalSchema = root.getOutSchema();
    Iterator<Column> it = finalSchema.getColumns().iterator();
    Column col = it.next();
    assertEquals("deptname", col.getSimpleName());
    col = it.next();
    assertEquals("total", col.getSimpleName());

    expr = sqlAnalyzer.parse(ALIAS[1]);
    plan = planner.createPlan(session, expr);
    root = plan.getRootBlock().getRoot();

    finalSchema = root.getOutSchema();
    it = finalSchema.getColumns().iterator();
    col = it.next();
    assertEquals("id", col.getSimpleName());
    col = it.next();
    assertEquals("total", col.getSimpleName());
  }

  @Test
  public final void testAlias2() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(ALIAS[1]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);

    Schema finalSchema = root.getOutSchema();
    Iterator<Column> it = finalSchema.getColumns().iterator();
    Column col = it.next();
    assertEquals("id", col.getSimpleName());
    col = it.next();
    assertEquals("total", col.getSimpleName());
  }

  static final String CREATE_TABLE [] = {
    "create external table table1 (name text, age int, earn bigint, score real) using csv with ('csv.delimiter'='|') location '/tmp/data'"
  };

  @Test
  public final void testCreateTableDef() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(CREATE_TABLE[0]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    assertEquals(NodeType.CREATE_TABLE, plan.getChild(root).getType());
    CreateTableNode createTable = plan.getChild(root);

    Schema def = createTable.getTableSchema();
    assertEquals("name", def.getColumn(0).getSimpleName());
    assertEquals(Type.TEXT, def.getColumn(0).getDataType().getType());
    assertEquals("age", def.getColumn(1).getSimpleName());
    assertEquals(Type.INT4, def.getColumn(1).getDataType().getType());
    assertEquals("earn", def.getColumn(2).getSimpleName());
    assertEquals(Type.INT8, def.getColumn(2).getDataType().getType());
    assertEquals("score", def.getColumn(3).getSimpleName());
    assertEquals(Type.FLOAT4, def.getColumn(3).getDataType().getType());
    assertEquals(StoreType.CSV, createTable.getStorageType());
    assertEquals("/tmp/data", createTable.getPath().toString());
    assertTrue(createTable.hasOptions());
    assertEquals("|", createTable.getOptions().get("csv.delimiter"));
  }

  private static final List<Set<Column>> testGenerateCuboidsResult
    = Lists.newArrayList();
  private static final int numCubeColumns = 3;
  private static final Column [] testGenerateCuboids = new Column[numCubeColumns];

  private static final List<Set<Column>> testCubeByResult
    = Lists.newArrayList();
  private static final Column [] testCubeByCuboids = new Column[2];
  static {
    testGenerateCuboids[0] = new Column("col1", Type.INT4);
    testGenerateCuboids[1] = new Column("col2", Type.INT8);
    testGenerateCuboids[2] = new Column("col3", Type.FLOAT4);

    testGenerateCuboidsResult.add(new HashSet<Column>());
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[1]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[2]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0],
        testGenerateCuboids[1]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0],
        testGenerateCuboids[2]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[1],
        testGenerateCuboids[2]));
    testGenerateCuboidsResult.add(Sets.newHashSet(testGenerateCuboids[0],
        testGenerateCuboids[1], testGenerateCuboids[2]));

    testCubeByCuboids[0] = new Column("employee.name", Type.TEXT);
    testCubeByCuboids[1] = new Column("employee.empid", Type.INT4);
    testCubeByResult.add(new HashSet<Column>());
    testCubeByResult.add(Sets.newHashSet(testCubeByCuboids[0]));
    testCubeByResult.add(Sets.newHashSet(testCubeByCuboids[1]));
    testCubeByResult.add(Sets.newHashSet(testCubeByCuboids[0],
        testCubeByCuboids[1]));
  }

  @Test
  public final void testGenerateCuboids() {
    Column [] columns = new Column[3];

    columns[0] = new Column("col1", Type.INT4);
    columns[1] = new Column("col2", Type.INT8);
    columns[2] = new Column("col3", Type.FLOAT4);

    List<Column[]> cube = LogicalPlanner.generateCuboids(columns);
    assertEquals(((int)Math.pow(2, numCubeColumns)), cube.size());

    Set<Set<Column>> cuboids = Sets.newHashSet();
    for (Column [] cols : cube) {
      cuboids.add(Sets.newHashSet(cols));
    }

    for (Set<Column> result : testGenerateCuboidsResult) {
      assertTrue(cuboids.contains(result));
    }
  }

  static final String setStatements [] = {
    "select deptName from employee where deptName like 'data%' union select deptName from score where deptName like 'data%'",
  };

  @Test
  public final void testSetPlan() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(setStatements[0]);
    LogicalPlan plan = planner.createPlan(session, expr);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    assertEquals(NodeType.ROOT, root.getType());
    assertEquals(NodeType.UNION, plan.getChild(root).getType());
    UnionNode union = plan.getChild(root);
    assertEquals(NodeType.PROJECTION, plan.getLeftChild(union).getType());
    assertEquals(NodeType.PROJECTION, plan.getRightChild(union).getType());
  }

  static final String [] setQualifiers = {
    "select name, empid from employee",
    "select distinct name, empid from employee",
    "select all name, empid from employee",
  };

  @Test
  public void testSetQualifier() throws PlanningException {
    Expr context = sqlAnalyzer.parse(setQualifiers[0]);
    LogicalPlan plan = planner.createPlan(session, context);
    LogicalNode root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    assertEquals(NodeType.ROOT, root.getType());
    assertEquals(NodeType.PROJECTION, plan.getChild(root).getType());
    ProjectionNode projectionNode = plan.getChild(root);
    assertEquals(NodeType.SCAN, plan.getChild(projectionNode).getType());

    context = sqlAnalyzer.parse(setQualifiers[1]);
    plan = planner.createPlan(session, context);
    root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    assertEquals(NodeType.ROOT, root.getType());
    assertEquals(NodeType.PROJECTION, plan.getChild(root).getType());
    projectionNode = plan.getChild(root);
    assertEquals(NodeType.GROUP_BY, plan.getChild(projectionNode).getType());

    context = sqlAnalyzer.parse(setQualifiers[2]);
    plan = planner.createPlan(session, context);
    root = plan.getRootBlock().getRoot();
    testJsonSerDerObject(plan, root);
    assertEquals(NodeType.PROJECTION, plan.getChild(root).getType());
    projectionNode = plan.getChild(root);
    assertEquals(NodeType.SCAN, plan.getChild(projectionNode).getType());
  }

  public void testJsonSerDerObject(LogicalPlan plan, LogicalNode rootNode) {
    String json = rootNode.toJson();
    LogicalNode fromJson = CoreGsonHelper.fromJson(json, LogicalNode.class);
    assertTrue("JSON (de) serialization equivalence check", rootNode.deepEquals(fromJson));
  }

  // Table descriptions
  //
  // employee (name text, empid int4, deptname text)
  // dept (deptname text, nameger text)
  // score (deptname text, score inet4)

  static final String [] insertStatements = {
      "insert into score select name from employee",                        // 0
      "insert into score select name, empid from employee",                 // 1
      "insert into employee (name, deptname) select * from dept",           // 2
      "insert into location '/tmp/data' select name, empid from employee",  // 3
      "insert overwrite into employee (name, deptname) select * from dept", // 4
      "insert overwrite into LOCATION '/tmp/data' select * from dept"       // 5
  };

  @Test
  public final void testInsertInto0() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(insertStatements[0]);
    LogicalPlan plan = planner.createPlan(session, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertFalse(insertNode.isOverwrite());
    assertTrue(insertNode.hasTargetTable());
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), insertNode.getTableName());
  }

  @Test
  public final void testInsertInto1() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(insertStatements[1]);
    LogicalPlan plan = planner.createPlan(session, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertFalse(insertNode.isOverwrite());
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "score"), insertNode.getTableName());
  }

  @Test
  public final void testInsertInto2() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(insertStatements[2]);
    LogicalPlan plan = planner.createPlan(session, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertFalse(insertNode.isOverwrite());
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), insertNode.getTableName());
    assertTrue(insertNode.hasTargetSchema());
    assertEquals(insertNode.getTargetSchema().getColumn(0).getSimpleName(), "name");
    assertEquals(insertNode.getTargetSchema().getColumn(1).getSimpleName(), "deptname");
  }

  @Test
  public final void testInsertInto3() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(insertStatements[3]);
    LogicalPlan plan = planner.createPlan(session, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertFalse(insertNode.isOverwrite());
    assertTrue(insertNode.hasPath());
  }

  @Test
  public final void testInsertInto4() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(insertStatements[4]);
    LogicalPlan plan = planner.createPlan(session, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertTrue(insertNode.isOverwrite());
    assertTrue(insertNode.hasTargetTable());
    assertEquals(CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, "employee"), insertNode.getTableName());
    assertTrue(insertNode.hasTargetSchema());
    assertEquals(insertNode.getTargetSchema().getColumn(0).getSimpleName(), "name");
    assertEquals(insertNode.getTargetSchema().getColumn(1).getSimpleName(), "deptname");
  }

  @Test
  public final void testInsertInto5() throws PlanningException {
    Expr expr = sqlAnalyzer.parse(insertStatements[5]);
    LogicalPlan plan = planner.createPlan(session, expr);
    assertEquals(1, plan.getQueryBlocks().size());
    InsertNode insertNode = getInsertNode(plan);
    assertTrue(insertNode.isOverwrite());
    assertTrue(insertNode.hasPath());
  }

  private static InsertNode getInsertNode(LogicalPlan plan) {
    LogicalRootNode root = plan.getRootBlock().getRoot();
    assertEquals(NodeType.INSERT, plan.getChild(root).getType());
    return plan.getChild(root);
  }
}