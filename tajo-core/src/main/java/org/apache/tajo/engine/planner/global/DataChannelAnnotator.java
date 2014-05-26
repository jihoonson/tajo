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

package org.apache.tajo.engine.planner.global;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.BasicEvalNodeVisitor;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.graph.DirectedGraphVisitor;
import org.apache.tajo.engine.planner.logical.*;

import java.util.*;

public class DataChannelAnnotator implements DirectedGraphVisitor<ExecutionBlock> {
  private final MasterPlan masterPlan;

  public DataChannelAnnotator(MasterPlan masterPlan) {
    this.masterPlan = masterPlan;
  }

  @Override
  public void visit(Stack<ExecutionBlock> stack, ExecutionBlock current) {
    ExecutionBlock parent = stack.peek();
    if (parent.hasJoin() || (parent.hasLimit() && parent.hasSort()) || parent.hasAgg()) {
      DataChannel channel = masterPlan.getChannel(current, parent);
      List<Integer> asideColumnIdxs = new ArrayList<Integer>();

      // get aside column indexes
      ColumnExtractorContext context = new ColumnExtractorContext();
      CoreSchemaExtractor extractor = new CoreSchemaExtractor();
      try {
        extractor.visit(context, null, null, current.getPlan(), new Stack<LogicalNode>());

        // parent's input schema
        Schema channelSchema = channel.getSchema();
        for (Column column : channelSchema.getColumns()) {
          if (!context.columns.contains(column)) {
            asideColumnIdxs.add(channelSchema.getColumnId(column.getQualifiedName()));
          }
        }
        channel.setAsideColumnIdxs(asideColumnIdxs);
      } catch (PlanningException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class ColumnExtractorContext {
    Set<Column> columns = new HashSet<Column>();
    public void addColumn(Column column) {
      this.columns.add(column);
    }
  }

  static class CoreSchemaExtractor extends BasicLogicalPlanVisitor<ColumnExtractorContext, LogicalNode> {

    @Override
    public LogicalNode visitSort(ColumnExtractorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, SortNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);
      for (SortSpec sortSpec : node.getSortKeys()) {
        context.addColumn(sortSpec.getSortKey());
      }
      LogicalNode result = visit(context, plan, block, node.getChild(), stack);
      stack.pop();
      return result;
    }

    @Override
    public LogicalNode visitHaving(ColumnExtractorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                                   Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);

      ColumnExtractor columnExtractor = new ColumnExtractor();
      columnExtractor.visitChild(context, node.getQual(), new Stack<EvalNode>());

      LogicalNode result = visit(context, plan, block, node.getChild(), stack);
      stack.pop();
      return result;
    }

    @Override
    public LogicalNode visitGroupBy(ColumnExtractorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                                    Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);

      for (Column column : node.getGroupingColumns()) {
        context.addColumn(column);
      }
      for (Target target : node.getTargets()) {
        context.addColumn(target.getNamedColumn());
      }

      LogicalNode result = visit(context, plan, block, node.getChild(), stack);
      stack.pop();
      return result;
    }

    @Override
    public LogicalNode visitDistinct(ColumnExtractorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, DistinctGroupbyNode node,
                                     Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);

      for (Column column : node.getGroupingColumns()) {
        context.addColumn(column);
      }
      for (Target target : node.getTargets()) {
        context.addColumn(target.getNamedColumn());
      }

      LogicalNode result = visit(context, plan, block, node.getChild(), stack);
      stack.pop();
      return result;
    }

    @Override
    public LogicalNode visitFilter(ColumnExtractorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, SelectionNode node,
                                   Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);

      ColumnExtractor columnExtractor = new ColumnExtractor();
      columnExtractor.visitChild(context, node.getQual(), new Stack<EvalNode>());

      LogicalNode result = visit(context, plan, block, node.getChild(), stack);
      stack.pop();
      return result;
    }

    @Override
    public LogicalNode visitJoin(ColumnExtractorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);

      ColumnExtractor columnExtractor = new ColumnExtractor();
      columnExtractor.visitChild(context, node.getJoinQual(), new Stack<EvalNode>());

      for (Target target : node.getTargets()) {
        context.addColumn(target.getNamedColumn());
      }

      LogicalNode result = visit(context, plan, block, node.getLeftChild(), stack);
      visit(context, plan, block, node.getRightChild(), stack);
      stack.pop();
      return result;
    }
  }

  static class ColumnExtractor extends BasicEvalNodeVisitor<ColumnExtractorContext, EvalNode> {

    @Override
    public EvalNode visitField(ColumnExtractorContext context, Stack<EvalNode> stack, FieldEval evalNode) {
      context.addColumn(evalNode.getColumnRef());
      return evalNode;
    }
  }
}
