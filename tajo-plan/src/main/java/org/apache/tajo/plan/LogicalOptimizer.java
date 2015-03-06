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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.ReflectionUtil;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.util.graph.DirectedGraphCursor;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.joinorder.FoundJoinOrder;
import org.apache.tajo.plan.joinorder.GreedyHeuristicJoinOrderAlgorithm;
import org.apache.tajo.plan.joinorder.JoinGraph;
import org.apache.tajo.plan.joinorder.JoinOrderAlgorithm;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.*;

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
      JoinGraphContext joinGraphContext = JoinGraphBuilder.buildJoinGraph(plan, block);

      // finding join order and restore remain filter order
      FoundJoinOrder order = joinOrderAlgorithm.findBestOrder(plan, block,
          joinGraphContext.joinGraph);

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

  private static class VertexPair extends Pair<String, String> {

    public VertexPair() {

    }

    public VertexPair(String tail, String head) {
      super(tail, head);
    }

    public void set(String tail, String head) {
      this.setFirst(tail);
      this.setSecond(head);
    }
  }

  /**
   * In an associative group, every vertex pair is associative.
   * Every vertex pair must be involved in an associative group.
   */
  private static class AssociativeGroup {
    Map<VertexPair, JoinNode> joinRelations = TUtil.newHashMap(); // set to store the join relationships
//    Map<String, LogicalNode> pidToLogicalNodeMap = TUtil.newHashMap();

    public void addJoinRelation(JoinNode joinNode, String leftRelationName, String rightRelationName) {
      VertexPair pair = new VertexPair(leftRelationName, rightRelationName);
      joinRelations.put(pair, joinNode);
//      pidToLogicalNodeMap.put(pair.getFirst(), joinNode.getLeftChild());
//      pidToLogicalNodeMap.put(pair.getSecond(), joinNode.getRightChild());
    }

//    public LogicalNode getLogicalNode(int pid) {
//      return pidToLogicalNodeMap.get(pid);
//    }
  }

  private static class JoinGraphContext {
    JoinGraph joinGraph = new JoinGraph();
    Set<EvalNode> quals = Sets.newHashSet();
//    Set<String> relationsForProduct = Sets.newHashSet();
    List<AssociativeGroup> associativeGroups = TUtil.newList(); // associative groups must be sorted with their creation order
    AssociativeGroup currentGroup;
    Map<String, RelationNode> vertexNameToRelation = TUtil.newHashMap();

    public JoinGraphContext() {
      newAssociativeGroup();
    }

    public AssociativeGroup newAssociativeGroup() {
      currentGroup = new AssociativeGroup();
      associativeGroups.add(currentGroup);
      return currentGroup;
    }
  }

  private static class JoinGraphBuilder extends BasicLogicalPlanVisitor<JoinGraphContext, LogicalNode> {
    private final static JoinGraphBuilder instance;

    static {
      instance = new JoinGraphBuilder();
    }

    /**
     * This is based on the assumtion that all join and filter conditions are placed on the right join and
     * scan operators. In other words, filter push down must be performed before this method.
     * Otherwise, this method may build incorrectly a join graph.
     */
    public static JoinGraphContext buildJoinGraph(LogicalPlan plan, LogicalPlan.QueryBlock block)
        throws PlanningException {
      JoinGraphContext joinGraphContext = new JoinGraphContext();
      instance.visit(joinGraphContext, plan, block);
      if (!joinGraphContext.associativeGroups.isEmpty()) {
        addJoinEdges(joinGraphContext, plan, block);
      }
      return joinGraphContext;
    }

    public LogicalNode visitFilter(JoinGraphContext context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                   SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
      super.visitFilter(context, plan, block, node, stack);
      context.quals.addAll(Lists.newArrayList(AlgebraicUtil.toConjunctiveNormalFormArray(node.getQual())));
      return node;
    }

    @Override
    public LogicalNode visitJoin(JoinGraphContext joinGraphContext, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 JoinNode joinNode, Stack<LogicalNode> stack)
        throws PlanningException {
      super.visitJoin(joinGraphContext, plan, block, joinNode, stack);
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


      // Find the largest associative group from the left
      if (isAssociative(joinNode)) {
        joinGraphContext.currentGroup.addJoinRelation(joinNode,
            getMostRightRelNameWithinLineage(plan, joinNode.getLeftChild()).getCanonicalName(),
            getMostLeftRelNameWithinLineage(plan, joinNode.getRightChild()).getCanonicalName());
      } else {
        // add join edges
        addJoinEdges(joinGraphContext, plan, block);

        // update the current group
        joinGraphContext.newAssociativeGroup();
      }

      return joinNode;
    }
  }

  private static void addJoinEdges(JoinGraphContext context, LogicalPlan plan, LogicalPlan.QueryBlock block)
      throws PlanningException {
    // Create join edges for every pair of relations within the current group
    for (Map.Entry<Pair<RelationNode,RelationNode>, JoinNode> eachEntry :
        populateVertexPairsWithinAssociativeGroup(plan, block, context).entrySet()) {
      context.joinGraph.addJoin(block, eachEntry.getKey().getFirst(), eachEntry.getKey().getSecond(),
          eachEntry.getValue());
    }
  }

  private static Map<Pair<RelationNode,RelationNode>, JoinNode> populateVertexPairsWithinAssociativeGroup(LogicalPlan plan,
                                                                                                          LogicalPlan.QueryBlock block,
                                                                                                          JoinGraphContext contex)
      throws PlanningException {
    RelationNode leftRelation, rightRelation;
    AssociativeGroup group = contex.currentGroup;
    Map<String, RelationNode> vertexNameToRelation = contex.vertexNameToRelation;
    Set<EvalNode> joinConditions = contex.quals;

    // make the unique vertex set
    for (JoinNode eachJoin : group.joinRelations.values()) {
      leftRelation = getMostRightRelNameWithinLineage(plan, eachJoin.getLeftChild());
      rightRelation = getMostLeftRelNameWithinLineage(plan, eachJoin.getRightChild());
      vertexNameToRelation.put(leftRelation.getCanonicalName(), leftRelation);
      vertexNameToRelation.put(rightRelation.getCanonicalName(), rightRelation);
    }

    // get or create join nodes for every vertex pairs
    Map<Pair<RelationNode,RelationNode>, JoinNode> populatedJoins = TUtil.newHashMap();
    VertexPair keyVertexPair = new VertexPair();;
    for (String tail : vertexNameToRelation.keySet()) {
      for (String head : vertexNameToRelation.keySet()) {
        if (tail == head) continue;
        keyVertexPair.set(tail, head);
        leftRelation = vertexNameToRelation.get(tail);
        rightRelation = vertexNameToRelation.get(head);
        JoinNode joinNode;
        if (group.joinRelations.containsKey(keyVertexPair)) {
          joinNode = group.joinRelations.get(keyVertexPair);
        } else {
          // If there are no existing join nodes, create a new join node for this relationship
          // Here, join conditions must be referred to decide the join type between INNER and CROSS.
          Set<EvalNode> conditionsForThisJoin = TUtil.newHashSet();
          for (EvalNode cond : joinConditions) {
            if (EvalTreeUtil.isJoinQual(block, leftRelation.getOutSchema(), rightRelation.getOutSchema(),
                cond, false)) {
              conditionsForThisJoin.add(cond);
            }
          }
          JoinType joinType;
          if (conditionsForThisJoin.isEmpty()) {
            joinType = JoinType.CROSS;
          } else {
            joinType = JoinType.INNER;
          }
          joinNode = createJoinNode(plan, joinType, leftRelation, rightRelation, conditionsForThisJoin);
        }
        populatedJoins.put(new Pair<RelationNode, RelationNode>(leftRelation, rightRelation), joinNode);
      }
    }

    return populatedJoins;
  }

  private static RelationNode getMostLeftRelNameWithinLineage(LogicalPlan plan, LogicalNode root) throws PlanningException {
    List<RelationNode> names = TUtil.newList(PlannerUtil.getRelationLineageWithinQueryBlock(plan, root));
    return names.get(0);
  }

  private static RelationNode getMostRightRelNameWithinLineage(LogicalPlan plan, LogicalNode root) throws PlanningException {
    List<RelationNode> names = TUtil.newList(PlannerUtil.getRelationLineageWithinQueryBlock(plan, root));
    return names.get(names.size()-1);
  }

  // TODO: remove this function, and create join spec
  private static JoinNode createJoinNode(LogicalPlan plan, JoinType joinType, LogicalNode left, LogicalNode right,
                                         Set<EvalNode> joinConditions) {
    JoinNode joinNode = plan.createNode(JoinNode.class);

    if (PlannerUtil.isCommutativeJoin(joinType)) {
      // if only one operator is relation
      if ((left instanceof RelationNode) && !(right instanceof RelationNode)) {
        // for left deep
        joinNode.init(joinType, right, left);
      } else {
        // if both operators are relation or if both are relations
        // we don't need to concern the left-right position.
        joinNode.init(joinType, left, right);
      }
    } else {
      joinNode.init(joinType, left, right);
    }

    Schema mergedSchema = SchemaUtil.merge(joinNode.getLeftChild().getOutSchema(),
        joinNode.getRightChild().getOutSchema());
    joinNode.setInSchema(mergedSchema);
    joinNode.setOutSchema(mergedSchema);
    if (joinConditions != null  && !joinConditions.isEmpty()) {
      joinNode.setJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
          joinConditions.toArray(new EvalNode[joinConditions.size()])));
    }
    joinNode.setTargets(PlannerUtil.schemaToTargets(mergedSchema));
    return joinNode;
  }

  /**
   * Associativity rules
   *
   * ==============================================================
   * Left-Hand Bracketed  | Right-Hand Bracketed  | Equivalence
   * ==============================================================
   * (A inner B) inner C  | A inner (B inner C)   | Equivalent
   * (A left B) inner C   | A left (B inner C)    | Not equivalent
   * (A right B) inner C  |	A right (B inner C)   | Equivalent
   * (A full B) inner C   |	A full (B inner C)	  | Not equivalent
   * (A inner B) left C	  | A inner (B left C)	  | Equivalent
   * (A left B) left C	  | A left (B left C)	    | Equivalent
   * (A right B) left C   |	A right (B left C)	  | Equivalent
   * (A full B) left C    |	A full (B left C)     | Equivalent
   * (A inner B) right C  |	A inner (B right C)   | Not equivalent
   * (A left B) right C   |	A left (B right C)    | Not equivalent
   * (A right B) right C  |	A right (B right C)   | Equivalent
   * (A full B) right C   |	A full (B right C)    |	Not equivalent
   * (A inner B) full C   |	A inner (B full C)    |	Not equivalent
   * (A left B) full C    |	A left (B full C)     |	Not equivalent
   * (A right B) full C   |	A right (B full C)    |	Equivalent
   * (A full B) full C    |	A full (B full C)     |	Equivalent
   * ========================================================
   */
  private static boolean isAssociative(JoinNode joinNode) {
    switch(joinNode.getJoinType()) {
      case LEFT_ANTI:
      case RIGHT_ANTI:
      case LEFT_SEMI:
      case RIGHT_SEMI:
      case LEFT_OUTER:
      case RIGHT_OUTER:
      case FULL_OUTER:
        return false;
      case INNER:
        // TODO: consider when a join qual involves columns from two or more tables
    }
    return true;
  }

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