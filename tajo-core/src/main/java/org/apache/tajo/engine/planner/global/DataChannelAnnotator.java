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

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
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

public class DataChannelAnnotator implements DirectedGraphVisitor<ExecutionBlockId> {
  private final MasterPlan masterPlan;

  public DataChannelAnnotator(MasterPlan masterPlan) {
    this.masterPlan = masterPlan;
  }

  @Override
  public void visit(Stack<ExecutionBlockId> stack, ExecutionBlockId currentId) {
    if (!stack.isEmpty()) {
      ExecutionBlockId parentId = stack.peek();
      ExecutionBlock parent = masterPlan.getExecBlock(parentId);
      ExecutionBlock current = masterPlan.getExecBlock(currentId);

      try {
        // require late shuffle?
        PreprocessContext preprocessContext = preprocess(masterPlan.getLogicalPlan(), parent);

        // if it requires late shuffle, extract columns for late shuffle
        if (preprocessContext.requireLateShuffle) {
          ColumnExtractorContext extractorContext = new ColumnExtractorContext();
          RequireColumnExtractor extractor = new RequireColumnExtractor();
          extractor.visit(extractorContext, null, null, preprocessContext.lastSelectableNode, new Stack<LogicalNode>());
          Set<Column> requireColumnsForSelectablePlan = extractorContext.columns;
          extractorContext = new ColumnExtractorContext();
          extractorContext.tranverseEndNode = preprocessContext.lastSelectableNode;
          extractor.visit(extractorContext, null, null, parent.getPlan(), new Stack<LogicalNode>());
          Set<Column> requireColumnsAfterSelectablePlan = extractorContext.columns;

          List<Integer> asideColumnIdxs = new ArrayList<Integer>();
          DataChannel channel = masterPlan.getChannel(current, parent);
          Schema channelSchema = channel.getSchema();
          for (Column column : channelSchema.getColumns()) {
            if (!requireColumnsForSelectablePlan.contains(column) &&
                requireColumnsAfterSelectablePlan.contains(column)) {
              asideColumnIdxs.add(channelSchema.getColumnId(column.getQualifiedName()));
            }
          }
          channel.setAsideColumnIdxs(asideColumnIdxs);
        }
      } catch (PlanningException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private PreprocessContext preprocess(LogicalPlan logicalPlan, ExecutionBlock executionBlock)
      throws PlanningException {
    // are there any nodes after a selectable node?
    // selectable nodes include join, sort with limit, scan, and having.

    GlobalPlanPreprocessor preprocessor = new GlobalPlanPreprocessor();
    PreprocessContext context = new PreprocessContext();
    context.hasSort = executionBlock.hasSort();
    preprocessor.visit(context, logicalPlan, null, executionBlock.getPlan(), new Stack<LogicalNode>());
    return context;
  }

  static class PreprocessContext {
    boolean hasSort = false;
    boolean requireLateShuffle = false;
    LogicalNode lastSelectableNode;
  }

  static class GlobalPlanPreprocessor extends BasicLogicalPlanVisitor<PreprocessContext, LogicalNode> {

    @Override
    public LogicalNode visit(PreprocessContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalNode node,
                        Stack<LogicalNode> stack)
        throws PlanningException {
      if (context.lastSelectableNode != null) {
        context.requireLateShuffle = true;
      }
      return super.visit(context, plan, block, node, stack);
    }

    @Override
    public LogicalNode visitHaving(PreprocessContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                                   Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);

      if (context.lastSelectableNode == null) {
        context.lastSelectableNode = node;
      }

      LogicalNode result = visit(context, plan, block, node.getChild(), stack);
      stack.pop();
      return result;
    }

    @Override
    public LogicalNode visitJoin(PreprocessContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);

      if (context.lastSelectableNode == null) {
        context.lastSelectableNode = node;
      }

      LogicalNode result = visit(context, plan, block, node.getLeftChild(), stack);
      visit(context, plan, block, node.getRightChild(), stack);
      stack.pop();
      return result;
    }

    @Override
    public LogicalNode visitScan(PreprocessContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
      if (context.lastSelectableNode == null) {
        context.lastSelectableNode = node;
      }
      return null;
    }

    @Override
    public LogicalNode visitLimit(PreprocessContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                  LimitNode node, Stack<LogicalNode> stack) throws PlanningException {
      stack.push(node);
      if (context.hasSort && context.lastSelectableNode == null) {
        context.lastSelectableNode = node;
      }
      LogicalNode result = visit(context, plan, block, node.getChild(), stack);
      stack.pop();
      return result;
    }
  }

  static class ColumnExtractorContext {
    Set<Column> columns = new HashSet<Column>();
    LogicalNode tranverseEndNode;
    public void addColumn(Column column) {
      this.columns.add(column);
    }
  }

  static class RequireColumnExtractor extends BasicLogicalPlanVisitor<ColumnExtractorContext, LogicalNode> {

    @Override
    public LogicalNode visit(ColumnExtractorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalNode node,
                             Stack<LogicalNode> stack)
        throws PlanningException {
      if (context.tranverseEndNode == null || !node.equals(context.tranverseEndNode)) {
        return super.visit(context, plan, block, node, stack);
      } else {
        return null;
      }
    }

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

//      for (Target target : node.getTargets()) {
//        context.addColumn(target.getNamedColumn());
//      }
      for (AggregationFunctionCallEval eval : node.getAggFunctions()) {
        ColumnExtractor columnExtractor = new ColumnExtractor();
        columnExtractor.visitChild(context, eval, new Stack<EvalNode>());
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

      if (node.hasJoinQual()) {
        ColumnExtractor columnExtractor = new ColumnExtractor();
        columnExtractor.visitChild(context, node.getJoinQual(), new Stack<EvalNode>());
      }

      // TODO: handle target
//      for (Target target : node.getTargets()) {
//        context.addColumn(target.getNamedColumn());
//      }

      LogicalNode result = visit(context, plan, block, node.getLeftChild(), stack);
      visit(context, plan, block, node.getRightChild(), stack);
      stack.pop();
      return result;
    }

    @Override
    public LogicalNode visitScan(ColumnExtractorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
      if (node.hasQual()) {
        ColumnExtractor columnExtractor = new ColumnExtractor();
        columnExtractor.visitChild(context, node.getQual(), new Stack<EvalNode>());
      }
      return null;
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
