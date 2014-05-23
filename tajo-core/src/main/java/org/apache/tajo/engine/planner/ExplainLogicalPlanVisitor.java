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

import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.engine.planner.logical.*;

import java.util.Stack;

/**
 * It returns a list of node plan strings.
 */
public class ExplainLogicalPlanVisitor extends BasicLogicalPlanVisitor<ExplainLogicalPlanVisitor.Context, LogicalNode> {

  public static class Context {
    public int maxDepth  = -1;
    public int depth = 0;
    public Stack<DepthString> explains = new Stack<DepthString>();

    public void add(int depth, PlanString planString) {
      maxDepth = Math.max(maxDepth, depth);
      explains.push(new DepthString(depth, planString));
    }

    public int getMaxDepth() {
      return this.maxDepth;
    }

    public Stack<DepthString> getExplains() {
      return explains;
    }
  }

  public static class DepthString {
    private int depth;
    private PlanString planStr;

    DepthString(int depth, PlanString planStr) {
      this.depth = depth;
      this.planStr = planStr;
    }

    public int getDepth() {
      return depth;
    }

    public PlanString getPlanString() {
      return planStr;
    }
  }

  public Context getBlockPlanStrings(@Nullable LogicalPlan plan,
                                     LogicalPlanTree planTree, LogicalNode node) throws PlanningException {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    Context explainContext = new Context();
    visit(explainContext, plan, null, planTree, node, stack);
    return explainContext;
  }

  @Override
  public LogicalNode visitRoot(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               LogicalPlanTree planTree, LogicalRootNode node, Stack<LogicalNode> stack)
      throws PlanningException {
//    return visit(context, plan, block, node.getChild(), stack);
    return visit(context, plan, block, planTree, planTree.getChild(node), stack);
  }

  @Override
  public LogicalNode visitProjection(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     LogicalPlanTree planTree, ProjectionNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return visitUnaryNode(context, plan, block, planTree, node, stack);
  }

  @Override
  public LogicalNode visitLimit(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                LogicalPlanTree planTree, LimitNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return visitUnaryNode(context, plan, block, planTree, node, stack);
  }

  @Override
  public LogicalNode visitSort(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                               SortNode node, Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, planTree, node, stack);
  }

  public LogicalNode visitHaving(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                                 HavingNode node, Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, planTree, node, stack);
  }

  @Override
  public LogicalNode visitGroupBy(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                                  GroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, planTree, node, stack);
  }

  @Override
  public LogicalNode visitDistinct(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                                   DistinctGroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, planTree, node, stack);
  }

  private LogicalNode visitUnaryNode(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     LogicalPlanTree planTree, LogicalNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    context.depth++;
    stack.push(node);
//    visit(context, plan, block, node.getChild(), stack);
    visit(context, plan, block, planTree, planTree.getChild(node), stack);
    context.depth--;
    context.add(context.depth, node.getPlanString());
    return node;
  }

  private LogicalNode visitBinaryNode(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                      LogicalPlanTree planTree, LogicalNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    context.depth++;
    stack.push(node);
//    visit(context, plan, block, node.getLeftChild(), stack);
//    visit(context, plan, block, node.getRightChild(), stack);
    visit(context, plan, block, planTree, planTree.getLeftChild(node), stack);
    visit(context, plan, block, planTree, planTree.getRightChild(node), stack);
    stack.pop();
    context.depth--;
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitFilter(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 LogicalPlanTree planTree, SelectionNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
    return visitUnaryNode(context, plan, block, planTree, node, stack);
  }

  @Override
  public LogicalNode visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               LogicalPlanTree planTree, JoinNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    return visitBinaryNode(context, plan, block, planTree, node, stack);
  }

  @Override
  public LogicalNode visitUnion(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                LogicalPlanTree planTree, UnionNode node,
                                Stack<LogicalNode> stack) throws PlanningException {
    return visitBinaryNode(context, plan, block, planTree, node, stack);
  }

  @Override
  public LogicalNode visitExcept(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 LogicalPlanTree planTree, ExceptNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
    return visitBinaryNode(context, plan, block, planTree, node, stack);
  }

  @Override
  public LogicalNode visitIntersect(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    LogicalPlanTree planTree, IntersectNode node,
                                    Stack<LogicalNode> stack) throws PlanningException {
    return visitBinaryNode(context, plan, block, planTree, node, stack);
  }

  @Override
  public LogicalNode visitTableSubQuery(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                        LogicalPlanTree planTree, TableSubQueryNode node,
                                        Stack<LogicalNode> stack) throws PlanningException {
    context.depth++;
    stack.push(node);
    visit(context, plan, block, planTree, node.getSubQuery(), new Stack<LogicalNode>());
    stack.pop();
    context.depth--;
    context.add(context.depth, node.getPlanString());

    return node;
  }

  @Override
  public LogicalNode visitScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               LogicalPlanTree planTree, ScanNode node,
                               Stack<LogicalNode> stack) throws PlanningException {
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitPartitionedTableScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                               LogicalPlanTree planTree, PartitionedTableScanNode node,
                                               Stack<LogicalNode> stack)
      throws PlanningException {
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitStoreTable(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     LogicalPlanTree planTree, StoreTableNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return visitUnaryNode(context, plan, block, planTree, node, stack);
  }

  public LogicalNode visitCreateDatabase(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                         LogicalPlanTree planTree, CreateDatabaseNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    context.add(context.depth, node.getPlanString());
    return node;
  }

  public LogicalNode visitDropDatabase(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                       LogicalPlanTree planTree, DropDatabaseNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    context.add(context.depth, node.getPlanString());
    return node;
  }

  @Override
  public LogicalNode visitInsert(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 LogicalPlanTree planTree, InsertNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
    context.depth++;
    stack.push(node);
    super.visitInsert(context, plan, block, planTree, node, stack);
    stack.pop();
    context.depth--;
    context.add(context.depth, node.getPlanString());
    return node;
  }

  public static String printDepthString(int maxDepth, DepthString planStr) {
    StringBuilder output = new StringBuilder();
    String pad = new String(new char[planStr.getDepth() * 3]).replace('\0', ' ');
    output.append(pad + planStr.getPlanString().getTitle()).append("\n");

    for (String str : planStr.getPlanString().getExplanations()) {
      output.append(pad).append("  => ").append(str).append("\n");
    }

    for (String str : planStr.getPlanString().getDetails()) {
      output.append(pad).append("  => ").append(str).append("\n");
    }
    return output.toString();
  }
}
