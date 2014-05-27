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

import org.apache.tajo.engine.planner.logical.*;

import java.util.Stack;

public class BasicLogicalPlanVisitor<CONTEXT, RESULT> implements LogicalPlanVisitor<CONTEXT, RESULT> {

  /**
   * The prehook is called before each node is visited.
   */
  @SuppressWarnings("unused")
  public void preHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, CONTEXT data)
      throws PlanningException {
  }

  /**
   * The posthook is called after each node is visited.
   */
  @SuppressWarnings("unused")
  public void postHook(LogicalPlan plan, LogicalNode node, Stack<LogicalNode> stack, CONTEXT data)
      throws PlanningException {
  }

  public CONTEXT visit(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block)
      throws PlanningException {
    visit(context, plan, block, plan.getPlanTree(), block.getRoot(), new Stack<LogicalNode>());
    return context;
  }

  /**
   * visit visits each logicalNode recursively.
   */
  public RESULT visit(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                      LogicalPlanTree planTree, LogicalNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    RESULT current;
    switch (node.getType()) {
      case ROOT:
        current = visitRoot(context, plan, block, planTree, (LogicalRootNode) node, stack);
        break;
      case EXPRS:
        return null;
      case PROJECTION:
        current = visitProjection(context, plan, block, planTree, (ProjectionNode) node, stack);
        break;
      case LIMIT:
        current = visitLimit(context, plan, block, planTree, (LimitNode) node, stack);
        break;
      case SORT:
        current = visitSort(context, plan, block, planTree, (SortNode) node, stack);
        break;
      case HAVING:
        current = visitHaving(context, plan, block, planTree, (HavingNode) node, stack);
        break;
      case GROUP_BY:
        current = visitGroupBy(context, plan, block, planTree, (GroupbyNode) node, stack);
        break;
      case DISTINCT_GROUP_BY:
        current = visitDistinct(context, plan, block, planTree, (DistinctGroupbyNode) node, stack);
        break;
      case SELECTION:
        current = visitFilter(context, plan, block, planTree, (SelectionNode) node, stack);
        break;
      case JOIN:
        current = visitJoin(context, plan, block, planTree, (JoinNode) node, stack);
        break;
      case UNION:
        current = visitUnion(context, plan, block, planTree, (UnionNode) node, stack);
        break;
      case EXCEPT:
        current = visitExcept(context, plan, block, planTree, (ExceptNode) node, stack);
        break;
      case INTERSECT:
        current = visitIntersect(context, plan, block, planTree, (IntersectNode) node, stack);
        break;
      case TABLE_SUBQUERY:
        current = visitTableSubQuery(context, plan, block, planTree, (TableSubQueryNode) node, stack);
        break;
      case SCAN:
        current = visitScan(context, plan, block, planTree, (ScanNode) node, stack);
        break;
      case PARTITIONS_SCAN:
        current = visitPartitionedTableScan(context, plan, block, planTree, (PartitionedTableScanNode) node, stack);
        break;
      case STORE:
        current = visitStoreTable(context, plan, block, planTree, (StoreTableNode) node, stack);
        break;
      case INSERT:
        current = visitInsert(context, plan, block, planTree, (InsertNode) node, stack);
        break;
      case CREATE_DATABASE:
        current = visitCreateDatabase(context, plan, block, planTree, (CreateDatabaseNode) node, stack);
        break;
      case DROP_DATABASE:
        current = visitDropDatabase(context, plan, block, planTree, (DropDatabaseNode) node, stack);
        break;
      case CREATE_TABLE:
        current = visitCreateTable(context, plan, block, planTree, (CreateTableNode) node, stack);
        break;
      case DROP_TABLE:
        current = visitDropTable(context, plan, block, planTree, (DropTableNode) node, stack);
        break;
      case ALTER_TABLESPACE:
        current = visitAlterTablespace(context, plan, block, planTree, (AlterTablespaceNode) node, stack);
        break;
      case ALTER_TABLE:
        current = visitAlterTable(context, plan, block, planTree, (AlterTableNode) node, stack);
        break;
      default:
        throw new PlanningException("Unknown logical node type: " + node.getType());
    }

    return current;
  }

  @Override
  public RESULT visitRoot(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                          LogicalRootNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitProjection(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                                ProjectionNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitLimit(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                           LimitNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitSort(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                          SortNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitHaving(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                            HavingNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitGroupBy(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                             GroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitDistinct(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                              DistinctGroupbyNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitFilter(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                            SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitJoin(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                          JoinNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getLeftChild(node), stack);
    visit(context, plan, block, planTree, planTree.getRightChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitUnion(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                           UnionNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    LogicalPlan.QueryBlock leftBlock = plan.getBlock(planTree.getLeftChild(node));
    RESULT result = visit(context, plan, leftBlock, planTree, leftBlock.getRoot(), stack);
    LogicalPlan.QueryBlock rightBlock = plan.getBlock(planTree.getRightChild(node));
    visit(context, plan, rightBlock, planTree, rightBlock.getRoot(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitExcept(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                            ExceptNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getLeftChild(node), stack);
    visit(context, plan, block, planTree, planTree.getRightChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitIntersect(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                               IntersectNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getLeftChild(node), stack);
    visit(context, plan, block, planTree, planTree.getRightChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitTableSubQuery(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   LogicalPlanTree planTree, TableSubQueryNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    stack.push(node);
    LogicalPlan.QueryBlock childBlock = plan.getBlock(node.getSubQuery());
    RESULT result = visit(context, plan, childBlock, planTree, childBlock.getRoot(), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitScan(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                          ScanNode node, Stack<LogicalNode> stack) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitPartitionedTableScan(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          LogicalPlanTree planTree, PartitionedTableScanNode node,
                                          Stack<LogicalNode> stack) throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitStoreTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                                StoreTableNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitInsert(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                            InsertNode node, Stack<LogicalNode> stack) throws PlanningException {
    stack.push(node);
    RESULT result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitCreateDatabase(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                    LogicalPlanTree planTree, CreateDatabaseNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitDropDatabase(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  LogicalPlanTree planTree, DropDatabaseNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitCreateTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                                 CreateTableNode node, Stack<LogicalNode> stack) throws PlanningException {
    RESULT result = null;
    stack.push(node);
//    if (node.hasSubQuery()) {
    if (planTree.hasChild(node)) {
      result = visit(context, plan, block, planTree, planTree.getChild(node), stack);
    }
    stack.pop();
    return result;
  }

  @Override
  public RESULT visitDropTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                               DropTableNode node, Stack<LogicalNode> stack) {
    return null;
  }

  @Override
  public RESULT visitAlterTablespace(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     LogicalPlanTree planTree, AlterTablespaceNode node, Stack<LogicalNode> stack)
      throws PlanningException {
    return null;
  }

  @Override
  public RESULT visitAlterTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                                AlterTableNode node, Stack<LogicalNode> stack) {
        return null;
    }
}
