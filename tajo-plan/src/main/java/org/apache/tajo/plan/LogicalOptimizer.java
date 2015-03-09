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

package org.apache.tajo.plan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.expr.EvalType;
import org.apache.tajo.plan.joinorder.*;
import org.apache.tajo.util.ReflectionUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.graph.DirectedGraphCursor;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static org.apache.tajo.plan.LogicalPlan.BlockEdge;
import static org.apache.tajo.plan.joinorder.GreedyHeuristicJoinOrderAlgorithm.getCost;

/**
 * This class optimizes a logical plan.
 */
@InterfaceStability.Evolving
public class LogicalOptimizer {
  private static final Log LOG = LogFactory.getLog(LogicalOptimizer.class.getName());

  private BaseLogicalPlanRewriteEngine rulesBeforeJoinOpt;
  private BaseLogicalPlanRewriteEngine rulesAfterToJoinOpt;
  private JoinOrderAlgorithm joinOrderAlgorithm = new GreedyHeuristicJoinOrderAlgorithm();

  public LogicalOptimizer(TajoConf conf) {

    Class clazz = conf.getClassVar(ConfVars.LOGICAL_PLAN_REWRITE_RULE_PROVIDER_CLASS);
    LogicalPlanRewriteRuleProvider provider = (LogicalPlanRewriteRuleProvider) ReflectionUtil.newInstance(clazz, conf);

    rulesBeforeJoinOpt = new BaseLogicalPlanRewriteEngine();
    rulesBeforeJoinOpt.addRewriteRule(provider.getPreRules());
    rulesAfterToJoinOpt = new BaseLogicalPlanRewriteEngine();
    rulesAfterToJoinOpt.addRewriteRule(provider.getPostRules());
  }

  public void addRuleAfterToJoinOpt(LogicalPlanRewriteRule rewriteRule) {
    if (rewriteRule != null) {
      rulesAfterToJoinOpt.addRewriteRule(rewriteRule);
    }
  }

  @VisibleForTesting
  public LogicalNode optimize(LogicalPlan plan) throws PlanningException {
    OverridableConf conf = new OverridableConf(new TajoConf(),
        ConfigKey.ConfigType.SESSION, ConfigKey.ConfigType.QUERY, ConfigKey.ConfigType.SYSTEM);
    return optimize(conf, plan);
  }

  public LogicalNode optimize(OverridableConf context, LogicalPlan plan) throws PlanningException {
    rulesBeforeJoinOpt.rewrite(context, plan);

    DirectedGraphCursor<String, BlockEdge> blockCursor =
        new DirectedGraphCursor<String, BlockEdge>(plan.getQueryBlockGraph(), plan.getRootBlock().getName());

    if (context == null || context.getBool(SessionVars.TEST_JOIN_OPT_ENABLED)) {
      // default is true
      while (blockCursor.hasNext()) {
        optimizeJoinOrder(plan, blockCursor.nextBlock());
      }
    } else {
      LOG.info("Skip Join Optimized.");
    }
    rulesAfterToJoinOpt.rewrite(context, plan);
    return plan.getRootBlock().getRoot();
  }

  private void optimizeJoinOrder(LogicalPlan plan, String blockName) throws PlanningException {
    LogicalPlan.QueryBlock block = plan.getBlock(blockName);

    if (block.hasNode(NodeType.JOIN)) {
      String originalOrder = JoinOrderStringBuilder.buildJoinOrderString(plan, block);
      double nonOptimizedJoinCost = JoinCostComputer.computeCost(plan, block);

      // finding relations and filter expressions
//      JoinGraphContext joinGraphContext = JoinGraphBuilder.buildJoinGraph(plan, block);

      JoinTreeContext joinTreeContext = JoinTreeBuilder.buildJoinTree(plan, block);
      Preconditions.checkState(joinTreeContext.joinVertexStack.size() == 1);
      JoinTree tree = new JoinTree(joinTreeContext.joinVertexStack.pop());

//      // finding join order and restore remain filter order
//      FoundJoinOrder order = joinOrderAlgorithm.findBestOrder(plan, block,
//          joinGraphContext.joinGraph, joinGraphContext.relationsForProduct);
      FoundJoinOrder order = null;

      // replace join node with FoundJoinOrder.
      JoinNode newJoinNode = order.getOrderedJoin();
      JoinNode old = PlannerUtil.findTopNode(block.getRoot(), NodeType.JOIN);

      JoinTargetCollector collector = new JoinTargetCollector();
      Set<Target> targets = new LinkedHashSet<Target>();
      collector.visitJoin(targets, plan, block, old, new Stack<LogicalNode>());

      if (targets.size() == 0) {
        newJoinNode.setTargets(PlannerUtil.schemaToTargets(old.getOutSchema()));
      } else {
        newJoinNode.setTargets(targets.toArray(new Target[targets.size()]));
      }
      PlannerUtil.replaceNode(plan, block.getRoot(), old, newJoinNode);
      // End of replacement logic

      String optimizedOrder = JoinOrderStringBuilder.buildJoinOrderString(plan, block);
      block.addPlanHistory("Non-optimized join order: " + originalOrder + " (cost: " + nonOptimizedJoinCost + ")");
      block.addPlanHistory("Optimized join order    : " + optimizedOrder + " (cost: " + order.getCost() + ")");
    }
  }

  private static class JoinTargetCollector extends BasicLogicalPlanVisitor<Set<Target>, LogicalNode> {
    @Override
    public LogicalNode visitJoin(Set<Target> ctx, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                                 Stack<LogicalNode> stack)
        throws PlanningException {
      super.visitJoin(ctx, plan, block, node, stack);

      if (node.hasTargets()) {
        for (Target target : node.getTargets()) {
          ctx.add(target);
        }
      }
      return node;
    }
  }

  private static class JoinTreeContext {
    private Stack<JoinVertex> joinVertexStack = new Stack<JoinVertex>();
    private Set<EvalNode> joinPredicateCandidates = TUtil.newHashSet();
  }

  private static class JoinTreeBuilder extends BasicLogicalPlanVisitor<JoinTreeContext, LogicalNode> {
    private final static JoinTreeBuilder instance;

    static {
      instance = new JoinTreeBuilder();
    }

    public static JoinTreeContext buildJoinTree(LogicalPlan plan, LogicalPlan.QueryBlock block)
        throws PlanningException {
      JoinTreeContext joinTreeContext = new JoinTreeContext();
      instance.visit(joinTreeContext, plan, block);
      return joinTreeContext;
    }

    @Override
    public LogicalNode visit(JoinTreeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
      if (node.getType() != NodeType.TABLE_SUBQUERY) {
        super.visit(context, plan, block, node, stack);
      }

      if (node instanceof RelationNode) {
        context.joinVertexStack.push(new RelationVertex((RelationNode) node));
      }

      return node;
    }

    @Override
    public LogicalNode visitFilter(JoinTreeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   SelectionNode selectionNode, Stack<LogicalNode> stack) throws PlanningException {
      // all join predicate candidates must be collected before building the join tree
      context.joinPredicateCandidates.addAll(
          TUtil.newList(AlgebraicUtil.toConjunctiveNormalFormArray(selectionNode.getQual())));
      super.visitFilter(context, plan, block, selectionNode, stack);
      return selectionNode;
    }

    @Override
    public LogicalNode visitJoin(JoinTreeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode joinNode, Stack<LogicalNode> stack) throws PlanningException {
      super.visitJoin(context, plan, block, joinNode, stack);
      Preconditions.checkState(context.joinVertexStack.size() > 1, "Some of join children are not found.");
      JoinVertex rightChild = context.joinVertexStack.pop();
      JoinVertex leftChild = context.joinVertexStack.pop();
      Preconditions.checkState(rightChild instanceof RelationVertex, "Right child must be the relation");
      Set<EvalNode> joinConditions = TUtil.newHashSet();
      if (joinNode.hasJoinQual()) {
        joinConditions.addAll(
            TUtil.newHashSet(AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual())));
      }

      if (PlannerUtil.isAssociativeJoin(joinNode)) {
        if (leftChild instanceof RelationVertex || leftChild instanceof NonAssociativeGroupVertex) {
          // create a new associative group
          AssociativeGroupVertex newGroup = new AssociativeGroupVertex();
          // add both children to the group
          newGroup.addVertex(leftChild);
          newGroup.addVertex(rightChild);
          // find all possible predicates for this join
          joinConditions.addAll(findJoinConditionForJoinVertex(context.joinPredicateCandidates, newGroup));
          newGroup.addPredicates(joinConditions);
          // push the new vertex into the stack
          context.joinVertexStack.push(newGroup);
        } else if (leftChild instanceof AssociativeGroupVertex) {
          // add the right child to the associative group
          AssociativeGroupVertex groupVertex = (AssociativeGroupVertex) leftChild;
          groupVertex.addVertex(rightChild);
          // find all possible predicates for this join
          joinConditions.addAll(findJoinConditionForJoinVertex(context.joinPredicateCandidates, groupVertex));
          groupVertex.addPredicates(joinConditions);
          // push the group vertex into the stack
          context.joinVertexStack.push(groupVertex);
        }
      } else {
        JoinEdge nonAssociativeEdge = new JoinEdge(joinNode.getJoinType(), leftChild, rightChild);
        nonAssociativeEdge.addJoinQuals(joinConditions);
        NonAssociativeGroupVertex newGroup = new NonAssociativeGroupVertex(nonAssociativeEdge);
        context.joinVertexStack.push(newGroup);
      }

      return joinNode;
    }
//
//    private static void addJoinEdges(JoinTreeContext context, LogicalPlan plan, LogicalPlan.QueryBlock block)
//        throws PlanningException {
//      // Create join edges for every pair of relations within the current group
//      for (Map.Entry<VertexPair, JoinEdge> eachEntry :
//          populateVertexPairsWithinAssociativeGroup(plan, block, context).entrySet()) {
//        context.currentAssociativeGroupVertex.addJoinEdge(eachEntry.getValue());
//      }
//    }

    private static Set<EvalNode> findJoinConditionForJoinVertex(Set<EvalNode> candidates,
                                                                AssociativeGroupVertex vertex) {
      Set<EvalNode> conditionsForThisJoin = TUtil.newHashSet();
      for (EvalNode predicate : candidates) {
        if (EvalTreeUtil.isJoinQual(predicate, false)
            && checkIfEvaluatedAtAssociatedGroup(predicate, vertex)) {
          conditionsForThisJoin.add(predicate);
        }
      }
      return conditionsForThisJoin;
    }

//    private static Map<VertexPair, JoinEdge> populateVertexPairsWithinAssociativeGroup(LogicalPlan plan,
//                                                                                       LogicalPlan.QueryBlock block,
//                                                                                       JoinTreeContext context)
//        throws PlanningException {
//      AssociativeGroupVertex group = context.currentAssociativeGroupVertex;
//
//      // get or create join nodes for every vertex pairs
//      Map<VertexPair, JoinEdge> populatedJoins = TUtil.newHashMap();
//      VertexPair keyVertexPair;
//
//      Set<JoinVertex> leftVertexes = TUtil.newHashSet();
//      Set<JoinVertex> rightVertexes = TUtil.newHashSet();
//      leftVertexes.addAll(group.getVertexes());
//      rightVertexes.addAll(group.getVertexes());
//      for (JoinEdge joinEdge : group.getJoinEdges()) {
//        if (!PlannerUtil.isCommutativeJoin(joinEdge.getJoinType())) {
//          rightVertexes.remove(joinEdge.getLeftRelation());
//          leftVertexes.remove(joinEdge.getRightRelation());
//        }
//      }
//
//      for (JoinVertex left : leftVertexes) {
//        for (JoinVertex right : rightVertexes) {
//          if (left.equals(right)) continue;
//          keyVertexPair = new VertexPair(left, right);
//          JoinEdge joinEdge;
//          if (group.getJoinEdgeMap().containsKey(keyVertexPair)) {
//            joinEdge = group.getJoinEdgeMap().get(keyVertexPair);
//          } else {
//            // If there are no existing join nodes, create a new join node for this relationship
//            joinEdge = createJoinEdge(JoinType.CROSS, left, right);
//          }
//          if (joinEdge.getJoinType() == JoinType.CROSS || joinEdge.getJoinType() == JoinType.INNER) {
//            // join conditions must be referred to decide the join type between INNER and CROSS.
//            // In addition, some join conditions can be moved to the optimal places due to the changed join order
//            Set<EvalNode> conditionsForThisJoin = TUtil.newHashSet();
//            for (EvalNode predicate : context.joinPredicateCandidates) {
//              if (EvalTreeUtil.isJoinQual(predicate, false)
//                  && checkIfEvaluatedAtAssociatedGroup(predicate, context.currentAssociativeGroupVertex)) {
//                conditionsForThisJoin.add(predicate);
//              }
//            }
//            if (!conditionsForThisJoin.isEmpty()) {
////              joinNode.setJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
////                  conditionsForThisJoin.toArray(new EvalNode[conditionsForThisJoin.size()])));
////              joinNode.setJoinType(JoinType.INNER);
//              joinEdge.setJoinType(JoinType.INNER);
//              joinEdge.addJoinQuals(conditionsForThisJoin);
//            }
//          }
//          populatedJoins.put(keyVertexPair, joinEdge);
//        }
//      }
//      return populatedJoins;
//    }
//
//    private static JoinEdge createJoinEdge(JoinType joinType, JoinVertex leftVertex, JoinVertex rightVertex) {
//      return new JoinEdge(joinType, leftVertex, rightVertex);
//    }

    private static boolean checkIfEvaluatedAtAssociatedGroup(EvalNode evalNode,
                                                             AssociativeGroupVertex group) {
      Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);

      if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
        return false;
      }

      if (EvalTreeUtil.findEvalsByType(evalNode, EvalType.WINDOW_FUNCTION).size() > 0) {
        return false;
      }

      if (columnRefs.size() > 0 && !group.getSchema().containsAll(columnRefs)) {
        return false;
      }
      return true;
    }
  }

//  private static class JoinGraphContext {
//    JoinGraph joinGraph = new JoinGraph();
//    Set<EvalNode> quals = Sets.newHashSet();
//    Set<String> relationsForProduct = Sets.newHashSet();
//  }
//
//  private static class JoinGraphBuilder extends BasicLogicalPlanVisitor<JoinGraphContext, LogicalNode> {
//    private final static JoinGraphBuilder instance;
//
//    static {
//      instance = new JoinGraphBuilder();
//    }
//
//    /**
//     * This is based on the assumtion that all join and filter conditions are placed on the right join and
//     * scan operators. In other words, filter push down must be performed before this method.
//     * Otherwise, this method may build incorrectly a join graph.
//     */
//    public static JoinGraphContext buildJoinGraph(LogicalPlan plan, LogicalPlan.QueryBlock block)
//        throws PlanningException {
//      JoinGraphContext joinGraphContext = new JoinGraphContext();
//      instance.visit(joinGraphContext, plan, block);
//      return joinGraphContext;
//    }
//
//    public LogicalNode visitFilter(JoinGraphContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
//                                   SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
//      super.visitFilter(context, plan, block, node, stack);
//      context.quals.addAll(Lists.newArrayList(AlgebraicUtil.toConjunctiveNormalFormArray(node.getQual())));
//      return node;
//    }
//
//    @Override
//    public LogicalNode visitJoin(JoinGraphContext joinGraphContext, LogicalPlan plan, LogicalPlan.QueryBlock block,
//                                 JoinNode joinNode, Stack<LogicalNode> stack)
//        throws PlanningException {
//      super.visitJoin(joinGraphContext, plan, block, joinNode, stack);
//      if (joinNode.hasJoinQual()) {
//        joinGraphContext.joinGraph.addJoin(plan, block, joinNode);
//      } else {
//        LogicalNode leftChild = joinNode.getLeftChild();
//        LogicalNode rightChild = joinNode.getRightChild();
//        if (leftChild instanceof RelationNode) {
//          RelationNode rel = (RelationNode) leftChild;
//          joinGraphContext.relationsForProduct.add(rel.getCanonicalName());
//        }
//        if (rightChild instanceof RelationNode) {
//          RelationNode rel = (RelationNode) rightChild;
//          joinGraphContext.relationsForProduct.add(rel.getCanonicalName());
//        }
//      }
//      return joinNode;
//    }
//  }

  public static class JoinOrderStringBuilder extends BasicLogicalPlanVisitor<StringBuilder, LogicalNode> {
    private static final JoinOrderStringBuilder instance;
    static {
      instance = new JoinOrderStringBuilder();
    }

    public static JoinOrderStringBuilder getInstance() {
      return instance;
    }

    public static String buildJoinOrderString(LogicalPlan plan, LogicalPlan.QueryBlock block) throws PlanningException {
      StringBuilder originalOrder = new StringBuilder();
      instance.visit(originalOrder, plan, block);
      return originalOrder.toString();
    }

    @Override
    public LogicalNode visitJoin(StringBuilder sb, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode joinNode,
                                 Stack<LogicalNode> stack)
        throws PlanningException {
      stack.push(joinNode);
      sb.append("(");
      visit(sb, plan, block, joinNode.getLeftChild(), stack);
      sb.append(" ").append(getJoinNotation(joinNode.getJoinType())).append(" ");
      visit(sb, plan, block, joinNode.getRightChild(), stack);
      sb.append(")");
      stack.pop();
      return joinNode;
    }

    private static String getJoinNotation(JoinType joinType) {
      switch (joinType) {
      case CROSS: return "⋈";
      case INNER: return "⋈θ";
      case LEFT_OUTER: return "⟕";
      case RIGHT_OUTER: return "⟖";
      case FULL_OUTER: return "⟗";
      case LEFT_SEMI: return "⋉";
      case RIGHT_SEMI: return "⋊";
      case LEFT_ANTI: return "▷";
      }
      return ",";
    }

    @Override
    public LogicalNode visitTableSubQuery(StringBuilder sb, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          TableSubQueryNode node, Stack<LogicalNode> stack) {
      sb.append(node.getTableName());
      return node;
    }

    public LogicalNode visitScan(StringBuilder sb, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                                 Stack<LogicalNode> stack) {
      sb.append(node.getTableName());
      return node;
    }
  }

  private static class CostContext {
    double accumulatedCost = 0;
  }

  public static class JoinCostComputer extends BasicLogicalPlanVisitor<CostContext, LogicalNode> {
    private static final JoinCostComputer instance;

    static {
      instance = new JoinCostComputer();
    }

    public static double computeCost(LogicalPlan plan, LogicalPlan.QueryBlock block) throws PlanningException {
      CostContext costContext = new CostContext();
      instance.visit(costContext, plan, block);
      return costContext.accumulatedCost;
    }

    @Override
    public LogicalNode visitJoin(CostContext joinGraphContext, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode joinNode, Stack<LogicalNode> stack)
        throws PlanningException {
      super.visitJoin(joinGraphContext, plan, block, joinNode, stack);

      double filterFactor = 1;
      if (joinNode.hasJoinQual()) {
        EvalNode [] quals = AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual());
        filterFactor = Math.pow(GreedyHeuristicJoinOrderAlgorithm.DEFAULT_SELECTION_FACTOR, quals.length);
      }

      if (joinNode.getLeftChild() instanceof RelationNode) {
        joinGraphContext.accumulatedCost = getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild())
            * filterFactor;
      } else {
        joinGraphContext.accumulatedCost = joinGraphContext.accumulatedCost +
            (joinGraphContext.accumulatedCost * getCost(joinNode.getRightChild()) * filterFactor);
      }

      return joinNode;
    }
  }
}