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

public interface LogicalPlanVisitor<CONTEXT, RESULT> {
  RESULT visitRoot(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                   LogicalRootNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitProjection(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                         ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitLimit(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                    LimitNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitSort(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                   SortNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitHaving(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                     HavingNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitGroupBy(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                      GroupbyNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitDistinct(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                       DistinctGroupbyNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitFilter(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                     SelectionNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitJoin(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                   JoinNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitUnion(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                    UnionNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitExcept(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                     ExceptNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitIntersect(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                        IntersectNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitTableSubQuery(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                            TableSubQueryNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitScan(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                   ScanNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitPartitionedTableScan(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   LogicalPlanTree planTree, PartitionedTableScanNode node, Stack<LogicalNode> stack)
      throws PlanningException;

  RESULT visitStoreTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                         StoreTableNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitInsert(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                     InsertNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitCreateDatabase(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                             CreateDatabaseNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitDropDatabase(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                           DropDatabaseNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitCreateTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                          CreateTableNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitDropTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                        DropTableNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitAlterTablespace(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                              AlterTablespaceNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitAlterTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                         AlterTableNode node, Stack<LogicalNode> stack) throws PlanningException;

  RESULT visitTruncateTable(CONTEXT context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalPlanTree planTree,
                            TruncateTableNode node, Stack<LogicalNode> stack) throws PlanningException;
}
