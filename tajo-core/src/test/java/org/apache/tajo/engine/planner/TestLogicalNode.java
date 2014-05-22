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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.planner.LogicalPlan.PidFactory;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalNode.LogicalNodeEdge;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.junit.Test;

import java.util.Collection;
import java.util.Stack;

import static org.junit.Assert.*;

public class TestLogicalNode {
  public static final void testCloneLogicalNode(LogicalNode n1) throws CloneNotSupportedException {
    LogicalNode copy = (LogicalNode) n1.clone();
    assertTrue(n1.deepEquals(copy));
  }

  @Test
  public void testEquals() {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);
    LogicalPlanTree tree = new LogicalPlanTree(new PidFactory());

    GroupbyNode groupbyNode = new GroupbyNode(0);
    groupbyNode.setGroupingColumns(new Column[]{schema.getColumn(1), schema.getColumn(2)});
    ScanNode scanNode = new ScanNode(0);
    scanNode.init(CatalogUtil.newTableDesc("in", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in")));

    GroupbyNode groupbyNode2 = new GroupbyNode(0);
    groupbyNode2.setGroupingColumns(new Column[]{schema.getColumn(1), schema.getColumn(2)});
    JoinNode joinNode = new JoinNode(0);
    ScanNode scanNode2 = new ScanNode(0);
    scanNode2.init(CatalogUtil.newTableDesc("in2", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in2")));

    tree.setChild(scanNode, groupbyNode);
    tree.setChild(joinNode, groupbyNode2);
    tree.setLeftChild(scanNode, joinNode);
    tree.setRightChild(scanNode2, joinNode);
//    groupbyNode.setChild(scanNode);
//    groupbyNode2.setChild(joinNode);
//    joinNode.setLeftChild(scanNode);
//    joinNode.setRightChild(scanNode2);

    assertTrue(groupbyNode.equals(groupbyNode2));
    assertFalse(groupbyNode.deepEquals(groupbyNode2));

    ScanNode scanNode3 = new ScanNode(0);
    scanNode3.init(CatalogUtil.newTableDesc("in", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in")));
//    groupbyNode2.setChild(scanNode3);
    tree.setChild(scanNode3, groupbyNode2);

    assertTrue(groupbyNode.equals(groupbyNode2));
    assertTrue(groupbyNode.deepEquals(groupbyNode2));
  }

  @Test
  public final void testLogicalPlanTreeClone() throws CloneNotSupportedException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("age", Type.INT2);
    LogicalPlanTree tree = new LogicalPlanTree(new PidFactory());

    GroupbyNode groupbyNode = new GroupbyNode(0);
    groupbyNode.setGroupingColumns(new Column[]{schema.getColumn(1), schema.getColumn(2)});
    ScanNode scanNode = new ScanNode(1);
    scanNode.init(CatalogUtil.newTableDesc("in", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in")));

    JoinNode joinNode = new JoinNode(2);
    joinNode.setJoinType(JoinType.CROSS);
    ScanNode scanNode2 = new ScanNode(3);
    scanNode2.init(CatalogUtil.newTableDesc("in2", schema, CatalogUtil.newTableMeta(StoreType.CSV), new Path("in2")));

    tree.setChild(joinNode, groupbyNode);
    tree.setLeftChild(scanNode, joinNode);
    tree.setRightChild(scanNode2, joinNode);

    LogicalPlanTree clone = (LogicalPlanTree) tree.clone();
    assertEqualTree(tree, clone);
  }

  private static void assertEqualTree(LogicalPlanTree t1, LogicalPlanTree t2) {
    Collection<LogicalNodeEdge> edges1 = t1.getEdgesAll();
    Collection<LogicalNodeEdge> edges2 = t2.getEdgesAll();
    assertEquals(edges1.size(), edges2.size());

    Stack<LogicalNode> s1 = new Stack<LogicalNode>();
    Stack<LogicalNode> s2 = new Stack<LogicalNode>();
    s1.push(t1.getRootNode());
    s2.push(t2.getRootNode());
    LogicalNode n1, n2;

    while (!s1.isEmpty() && !s2.isEmpty()) {
      n1 = s1.pop();
      n2 = s2.pop();
      assertEquals(n1, n2);
      for (LogicalNode child : t1.getChilds(n1)) {
        s1.push(child);
      }
      for (LogicalNode child : t2.getChilds(n2)) {
        s2.push(child);
      }
    }
  }
}
