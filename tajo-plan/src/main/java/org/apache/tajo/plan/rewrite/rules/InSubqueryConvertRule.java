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

package org.apache.tajo.plan.rewrite.rules;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Stack;

public class InSubqueryConvertRule
    extends BasicLogicalPlanVisitor<InSubqueryConvertRule.InSubqueryConvertContext, LogicalNode>
    implements LogicalPlanRewriteRule{
  private final static Log LOG = LogFactory.getLog(InSubqueryConvertRule.class);
  private final static String NAME = "InSubQueryConvert";

  static class InSubqueryConvertContext {
    OverridableConf queryContext;

    public InSubqueryConvertContext(OverridableConf queryContext) {
      this.queryContext = queryContext;
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, LogicalPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      SelectionNode selectionNode = block.getNode(NodeType.SELECTION);
      if (selectionNode != null && selectionNode.hasQual()) {
        if (EvalTreeUtil.findEvalsByType(selectionNode.getQual(), EvalType.SUB_QUERY).size() > 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(OverridableConf queryContext, LogicalPlan plan) throws PlanningException {
    InSubqueryConvertContext context = new InSubqueryConvertContext(queryContext);
    this.visit(context, plan, plan.getRootBlock(), plan.getRootBlock().getRoot(), new Stack<LogicalNode>());
    return plan;
  }

  /**
   * Replace the filter with a join.
   *
   * @param context
   * @param plan
   * @param block
   * @param selectionNode
   * @param stack
   * @return
   * @throws PlanningException
   */
  @Override
  public LogicalNode visitFilter(InSubqueryConvertContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode selectionNode, Stack<LogicalNode> stack) throws PlanningException {
    for (EvalNode eval : EvalTreeUtil.findEvalsByType(selectionNode.getQual(), EvalType.IN)) {
      InEval inEval = (InEval) eval;
      if (inEval.getRightExpr().getType() == EvalType.SUB_QUERY) {
        SubQueryEval subQueryEval = inEval.getValueSet();

        // visit the inner subquery first because it can contain another IN subquery
        LogicalPlan.QueryBlock childBlock = plan.getBlock(subQueryEval.getSubQueryBlockName());
        visit(context, plan, childBlock, childBlock.getRoot(), new Stack<LogicalNode>());

        // create a new join node which will replace the filter node
        JoinNode joinNode = plan.createNode(JoinNode.class);
        JoinType joinType = inEval.isNot() ? JoinType.LEFT_OUTER : JoinType.INNER;
        joinNode.setJoinType(joinType);

        // the left and right children are the child of the filter and the inner subquery, respectively.
        joinNode.setLeftChild(selectionNode.getChild());
        joinNode.setRightChild(subQueryEval.getSubQueryNode());
        ProjectionNode projectionNode = PlannerUtil.findTopNode(subQueryEval.getSubQueryNode(), NodeType.PROJECTION);
        projectionNode.setDistinct(true);
        insertDistinctOperator(plan, childBlock, projectionNode, projectionNode.getChild());
//        joinNode.setRightChild(subQueryEval.getSubQueryNode());

        // connect the new join with the parent of the filter
        LogicalNode parent = PlannerUtil.findTopParentNode(block.getRoot(), NodeType.SELECTION);
        if (parent instanceof UnaryNode) {
          ((UnaryNode) parent).setChild(joinNode);
        } else if (parent instanceof BinaryNode) {
          BinaryNode binaryParent = (BinaryNode) parent;
          if (binaryParent.getLeftChild().getType() == NodeType.SELECTION) {
            binaryParent.setLeftChild(joinNode);
          } else if (binaryParent.getRightChild().getType() == NodeType.SELECTION) {
            binaryParent.setRightChild(joinNode);
          } else {
            throw new PlanningException("No such node who has the filter as its child");
          }
        } else {
          throw new PlanningException("No such node who has the filter as its child");
        }

        Schema inSchema = SchemaUtil.merge(selectionNode.getInSchema(), childBlock.getSchema());
        joinNode.setInSchema(inSchema);
        joinNode.setOutSchema(selectionNode.getOutSchema());

        List<Target> targets = TUtil.newList(PlannerUtil.schemaToTargets(inSchema));
        joinNode.setTargets(targets.toArray(new Target[targets.size()]));

        // join condition
        EvalNode joinCondition = buildJoinCondition(plan, inEval,
            joinNode.getRightChild().getOutSchema());
        joinNode.setJoinQual(joinCondition);
        block.addJoinType(joinType);
        block.registerNode(joinNode);
        plan.addHistory("IN subquery is optimized.");
        return joinNode;
      }
    }

    return null;
  }

//  private TableSubQueryNode createTableSubQueryNode(InSubqueryConvertContext context, LogicalPlan plan,
//                                                    LogicalPlan.QueryBlock queryBlock, LogicalNode child) {
//    TableSubQueryNode node = plan.createNode(TableSubQueryNode.class);
//    node.init(CatalogUtil.buildFQName(context.queryContext.get(SessionVars.CURRENT_DATABASE), "?" + queryBlock.getName()),
//        child);
//    queryBlock.addRelation(node);
//    return node;
//  }

//  private TableSubQueryNode insertDistinct(LogicalPlan plan, LogicalPlan.QueryBlock block,
//                                           LogicalPlan.QueryBlock childBlock,
//                                           ProjectionNode projectionNode, LogicalNode child) {
//    Schema outSchema = subQueryNode.getOutSchema();
//    GroupbyNode dupRemoval = plan.createNode(GroupbyNode.class);
//    dupRemoval.setChild(child);
//    dupRemoval.setInSchema(subQueryNode.getInSchema());
//    dupRemoval.setTargets(PlannerUtil.schemaToTargets(outSchema));
//    dupRemoval.setGroupingColumns(outSchema.toArray());
//
//    childBlock.registerNode(dupRemoval);
//
//    subQueryNode.setSubQuery(dupRemoval);
//    subQueryNode.setInSchema(dupRemoval.getOutSchema());
//    return subQueryNode;
//  }

  private void insertDistinctOperator(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                      ProjectionNode projectionNode, LogicalNode child) throws PlanningException {
    Schema outSchema = projectionNode.getOutSchema();
    GroupbyNode dupRemoval = plan.createNode(GroupbyNode.class);
    dupRemoval.setChild(child);
    dupRemoval.setInSchema(projectionNode.getInSchema());
    dupRemoval.setTargets(PlannerUtil.schemaToTargets(outSchema));
    dupRemoval.setGroupingColumns(outSchema.toArray());

    block.registerNode(dupRemoval);

    projectionNode.setChild(dupRemoval);
    projectionNode.setInSchema(dupRemoval.getOutSchema());
  }

  /**
   * Create the join condition.
   * The created join condition checks whether the predicand of the InEval equals the result of the inner query.
   *
   * @param plan
   * @param inEval
   * @return
   */
  private EvalNode buildJoinCondition(LogicalPlan plan, InEval inEval, Schema valueListSchema) {
    SubQueryEval subQueryEval = inEval.getValueSet();
    Preconditions.checkArgument(valueListSchema.size() == 1, "The schema size of the IN subquery must be 1");
    FieldEval fieldEval = new FieldEval(valueListSchema.getColumn(0));
    BinaryEval equalEval = new BinaryEval(EvalType.EQUAL, inEval.getPredicand(), fieldEval);
    return equalEval;
  }

//  /**
//   * Add a distinct node.
//   *
//   * @param context
//   * @param plan
//   * @param block
//   * @param subQueryNode
//   * @param stack
//   * @return
//   * @throws PlanningException
//   */
//  @Override
//  public LogicalNode visitTableSubQuery(InSubqueryConvertContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
//                                        TableSubQueryNode subQueryNode, Stack<LogicalNode> stack)
//      throws PlanningException {
//    return null;
//  }
}
