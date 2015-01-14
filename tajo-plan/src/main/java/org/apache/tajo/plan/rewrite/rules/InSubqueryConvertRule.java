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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.Stack;

public class InSubqueryConvertRule
    extends BasicLogicalPlanVisitor<InSubqueryConvertRule.InSubqueryConvertContext, LogicalNode>
    implements LogicalPlanRewriteRule{
  private final static Log LOG = LogFactory.getLog(InSubqueryConvertRule.class);
  private final static String NAME = "InSubQueryConvert";

  static class InSubqueryConvertContext {

  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, LogicalPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      SelectionNode selectionNode = block.getNode(NodeType.SELECTION);
      if (selectionNode.hasQual()) {
        if (EvalTreeUtil.findEvalsByType(selectionNode.getQual(), EvalType.SUB_QUERY).size() > 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(OverridableConf queryContext, LogicalPlan plan) throws PlanningException {
    return null;
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
    if (selectionNode.getQual().getType() == EvalType.IN) {
      InEval inEval = (InEval) selectionNode.getQual();
      if (inEval.getLeftExpr().getType() == EvalType.SUB_QUERY) {
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

        // join condition
        EvalNode joinCondition = null;
      }
    }

    return null;
  }

  /**
   * Add a distinct node.
   *
   * @param context
   * @param plan
   * @param block
   * @param subQueryNode
   * @param stack
   * @return
   * @throws PlanningException
   */
  @Override
  public LogicalNode visitTableSubQuery(InSubqueryConvertContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                        TableSubQueryNode subQueryNode, Stack<LogicalNode> stack)
      throws PlanningException {
    return null;
  }
}
