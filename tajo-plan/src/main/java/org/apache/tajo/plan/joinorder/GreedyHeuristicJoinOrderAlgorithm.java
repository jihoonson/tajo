/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.joinorder;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * This is a greedy heuristic algorithm to find a bushy join tree. This algorithm finds
 * the best join order with join conditions and pushed-down join conditions to
 * all join operators.
 */
public class GreedyHeuristicJoinOrderAlgorithm implements JoinOrderAlgorithm {
  public static double DEFAULT_SELECTION_FACTOR = 0.1;

  @Override
  public FoundJoinOrder findBestOrder(LogicalPlan plan, LogicalPlan.QueryBlock block, JoinTree joinTree)
      throws PlanningException {

    JoinTreeVisitorContext context = new JoinTreeVisitorContext(plan, block);
    JoinTreeVisitor visitor = new JoinTreeVisitor();
    visitor.visit(context, new Stack<JoinVertex>(), joinTree.getRoot());

    // all generated nodes should be registered to corresponding blocks
    block.registerNode(context.latestJoin);
    return new FoundJoinOrder(context.latestJoin, getCost(context.latestJoin));

//    // Setup a remain relation set to be joined
//    // Why we should use LinkedHashSet? - it should keep the deterministic for the order of joins.
//    // Otherwise, join orders can be different even if join costs are the same to each other.
//    Set<LogicalNode> remainRelations = new LinkedHashSet<LogicalNode>();
//    for (RelationNode relation : block.getRelations()) {
//      remainRelations.add(relation);
//    }
//
//    LogicalNode latestJoin;
//    JoinEdge bestPair;
//
//    while (remainRelations.size() > 1) {
//      Set<LogicalNode> checkingRelations = new LinkedHashSet<LogicalNode>();
//
//      for (LogicalNode relation : remainRelations) {
//        Collection <String> relationStrings = PlannerUtil.getRelationLineageWithinQueryBlock(plan, relation);
//        List<JoinEdge> joinEdges = new ArrayList<JoinEdge>();
//        String relationCollection = TUtil.collectionToString(relationStrings, ",");
//        List<JoinEdge> joinEdgesForGiven = joinGraph.getIncomingEdges(relationCollection);
//        if (joinEdgesForGiven != null) {
//          joinEdges.addAll(joinEdgesForGiven);
//        }
//        if (relationStrings.size() > 1) {
//          for (String relationString: relationStrings) {
//            joinEdgesForGiven = joinGraph.getIncomingEdges(relationString);
//            if (joinEdgesForGiven != null) {
//              joinEdges.addAll(joinEdgesForGiven);
//            }
//          }
//        }
//
//        // check if the relation is the last piece of outer join
//        boolean endInnerRelation = false;
//        for (JoinEdge joinEdge: joinEdges) {
//          switch(joinEdge.getJoinType()) {
//            case LEFT_ANTI:
//            case RIGHT_ANTI:
//            case LEFT_SEMI:
//            case RIGHT_SEMI:
//            case LEFT_OUTER:
//            case RIGHT_OUTER:
//            case FULL_OUTER:
//              endInnerRelation = true;
//              if (checkingRelations.size() <= 1) {
//                checkingRelations.add(relation);
//              }
//              break;
//          }
//        }
//
//        if (endInnerRelation) {
//          break;
//        }
//
//        checkingRelations.add(relation);
//      }
//
//      remainRelations.removeAll(checkingRelations);
//
//      // Find the best join pair among all joinable operators in candidate set.
//      while (checkingRelations.size() > 1) {
//        LinkedHashSet<String[]> removingJoinEdges = new LinkedHashSet<String[]>();
//        bestPair = getBestPair(plan, joinGraph, checkingRelations, removingJoinEdges);
//
//        checkingRelations.remove(bestPair.getLeftVertex());
//        checkingRelations.remove(bestPair.getRightVertex());
//        for (String[] joinEdge: removingJoinEdges) {
//          // remove the edge of the best pair from join graph
//          joinGraph.removeEdge(joinEdge[0], joinEdge[1]);
//        }
//
//        latestJoin = createJoinNode(plan, bestPair);
//        checkingRelations.add(latestJoin);
//
//        // all logical nodes should be registered to corresponding blocks
//        block.registerNode(latestJoin);
//      }
//
//      // new Logical block should be the first entry of new Set
//      checkingRelations.addAll(remainRelations);
//      remainRelations = checkingRelations;
//    }
//
//    JoinNode joinTree = (JoinNode) remainRelations.iterator().next();
//    // all generated nodes should be registered to corresponding blocks
//    block.registerNode(joinTree);
//    return new FoundJoinOrder(joinTree, getCost(joinTree));
  }

  private static class JoinTreeVisitorContext {
    private LogicalPlan plan;
    private LogicalPlan.QueryBlock block;
    private JoinNode latestJoin;

    public JoinTreeVisitorContext(LogicalPlan plan, LogicalPlan.QueryBlock block) {
      this.plan = plan;
      this.block = block;
    }
  }

  private static class JoinTreeVisitor {

    public void visit(JoinTreeVisitorContext context, Stack<JoinVertex> stack, JoinVertex vertex) {
      stack.push(vertex);
      if (vertex instanceof RelationVertex) {
        visitRelationVertex(context, stack, (RelationVertex) vertex);
      } else if (vertex instanceof AssociativeGroupVertex) {
        visitAssociativeGroupVertex(context, stack, (AssociativeGroupVertex) vertex);
      } else if (vertex instanceof NonAssociativeGroupVertex) {
        visitNonAssociativeGroupVertex(context, stack, (NonAssociativeGroupVertex) vertex);
      }
      stack.pop();
    }

    private void visitRelationVertex(JoinTreeVisitorContext context, Stack<JoinVertex> stack,
                                     RelationVertex vertex) {

    }

    private void visitAssociativeGroupVertex(JoinTreeVisitorContext context, Stack<JoinVertex> stack,
                                             AssociativeGroupVertex vertex) {
      for (JoinVertex nestedVertex : vertex.getVertexes()) {
        visit(context, stack, nestedVertex);
      }

      while (vertex.size() > 1) {
        JoinEdge bestPair = getBestPair(vertex);
        removePairFromGroup(vertex, bestPair);
        context.latestJoin = createJoinNode(context.plan, bestPair);

        // all logical nodes should be registered to corresponding blocks
        context.block.registerNode(context.latestJoin);

        // Even though the join type is associative, they are treated as a non-associative type
        // because the join order is fixed.
        NonAssociativeGroupVertex newVertex = new NonAssociativeGroupVertex(bestPair);
        newVertex.setJoinNode(context.latestJoin);
        vertex.addVertex(newVertex);
      }
    }

    private void visitNonAssociativeGroupVertex(JoinTreeVisitorContext context, Stack<JoinVertex> stack,
                                                NonAssociativeGroupVertex vertex) {
      visit(context, stack, vertex.getJoinEdge().getLeftVertex());
      visit(context, stack, vertex.getJoinEdge().getRightVertex());
      context.latestJoin = createJoinNode(context.plan, vertex.getJoinEdge());
      context.block.registerNode(context.latestJoin);
    }
  }

  private static JoinNode createJoinNode(LogicalPlan plan, JoinEdge joinEdge) {
    LogicalNode left = joinEdge.getLeftVertex().getCorrespondingNode();
    LogicalNode right = joinEdge.getRightVertex().getCorrespondingNode();

    JoinNode joinNode = plan.createNode(JoinNode.class);

    if (PlannerUtil.isCommutativeJoin(joinEdge.getJoinType())) {
      // if only one operator is relation
      if ((left instanceof RelationNode) && !(right instanceof RelationNode)) {
        // for left deep
        joinNode.init(joinEdge.getJoinType(), right, left);
      } else {
        // if both operators are relation or if both are relations
        // we don't need to concern the left-right position.
        joinNode.init(joinEdge.getJoinType(), left, right);
      }
    } else {
      joinNode.init(joinEdge.getJoinType(), left, right);
    }

    Schema mergedSchema = SchemaUtil.merge(joinNode.getLeftChild().getOutSchema(),
        joinNode.getRightChild().getOutSchema());
    joinNode.setInSchema(mergedSchema);
    joinNode.setOutSchema(mergedSchema);
    if (joinEdge.hasJoinQual()) {
      joinNode.setJoinQual(AlgebraicUtil.createSingletonExprFromCNF(joinEdge.getJoinQual()));
    }
    return joinNode;
  }

  /**
   * Find the best join pair among all joinable operators in candidate set.
   *
   * @return The best join pair among them
   * @throws PlanningException
   */
  public static JoinEdge getBestPair(AssociativeGroupVertex vertex) {
    double minCost = Double.MAX_VALUE;
    double minNonCrossJoinCost = Double.MAX_VALUE;
    JoinEdge bestJoin = null;
    JoinEdge bestNonCrossJoin = null;

    for (JoinEdge candidate : vertex.getJoinEdges()) {
      double cost = getCost(candidate);

      if (cost < minCost) {
        minCost = cost;
        bestJoin = candidate;
      }

      // Keep the min cost join
      // But, if there exists a qualified join, the qualified join must be chosen
      // rather than cross join regardless of cost.
      if (candidate.hasJoinQual()) {
        if (cost < minNonCrossJoinCost) {
          minNonCrossJoinCost = cost;
          bestNonCrossJoin = candidate;
        }
      }
    }

    if (bestNonCrossJoin != null) {
      return bestNonCrossJoin;
    } else {
      return bestJoin;
    }
  }

  private static void removePairFromGroup(AssociativeGroupVertex vertex, JoinEdge edge) {
    vertex.removeVertex(edge.getLeftVertex());
    vertex.removeVertex(edge.getRightVertex());
    vertex.removePredicates(TUtil.newHashSet(edge.getJoinQual()));
  }

//  private JoinEdge getBestPair(LogicalPlan plan, JoinGraph graph, Set<LogicalNode> candidateSet, Set<String[]> bestJoinEdges)
//      throws PlanningException {
//    double minCost = Double.MAX_VALUE;
//    JoinEdge bestJoin = null;
//    LinkedHashSet<String[]> relatedJoinEdges = null;
//    LinkedHashSet<String[]> relatedNonCrossJoinEdges = null;
//
//    double minNonCrossJoinCost = Double.MAX_VALUE;
//    JoinEdge bestNonCrossJoin = null;
//
//    for (LogicalNode outer : candidateSet) {
//      for (LogicalNode inner : candidateSet) {
//        if (outer.equals(inner)) {
//          continue;
//        }
//
//        LinkedHashSet<String[]> joinEdgePairs = new LinkedHashSet<String[]>();
//        JoinEdge foundJoin = findJoin(plan, graph, outer, inner, joinEdgePairs);
//        if (foundJoin == null) {
//          continue;
//        }
//        double cost = getCost(foundJoin);
//
//        if (cost < minCost) {
//          minCost = cost;
//          bestJoin = foundJoin;
//          relatedJoinEdges = joinEdgePairs;
//        }
//
//        // Keep the min cost join
//        // But, if there exists a qualified join, the qualified join must be chosen
//        // rather than cross join regardless of cost.
//        if (foundJoin.hasJoinQual()) {
//          if (cost < minNonCrossJoinCost) {
//            minNonCrossJoinCost = cost;
//            bestNonCrossJoin = foundJoin;
//            relatedNonCrossJoinEdges = joinEdgePairs;
//          }
//        }
//      }
//    }
//
//    if (bestNonCrossJoin != null) {
//      bestJoinEdges.addAll(relatedNonCrossJoinEdges);
//      return bestNonCrossJoin;
//    } else {
//      bestJoinEdges.addAll(relatedJoinEdges);
//      return bestJoin;
//    }
//  }

  /**
   * Find a join between two logical operator trees
   *
   * @return If there is no join condition between two relation, it returns NULL value.
   */
  private static JoinEdge findJoin(LogicalPlan plan, JoinGraph graph, LogicalNode outer, LogicalNode inner, Set<String[]> joinEdgePairs)
      throws PlanningException {
    JoinEdge foundJoinEdge = null;

    // If outer is outer join, make edge key using all relation names in outer.
//    SortedSet<String> relationNames =
//        new TreeSet<String>(PlannerUtil.getRelationLineageWithinQueryBlock(plan, outer));
//    String outerEdgeKey = TUtil.collectionToString(relationNames, ", ");
//    for (String innerName : PlannerUtil.getRelationLineageWithinQueryBlock(plan, inner)) {
//      if (graph.hasEdge(outerEdgeKey, innerName)) {
//        JoinEdge existJoinEdge = graph.getEdge(outerEdgeKey, innerName);
//        String[] joinEdgePair = {outerEdgeKey, innerName};
//        joinEdgePairs.add(joinEdgePair);
//        if (foundJoinEdge == null) {
//          foundJoinEdge = new JoinEdge(existJoinEdge.getJoinType(), outer, inner,
//              existJoinEdge.getJoinQual());
//        } else {
//          foundJoinEdge.addJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
//              existJoinEdge.getJoinQual()));
//        }
//      }
//    }
//    if (foundJoinEdge != null) {
//      return foundJoinEdge;
//    }
//
//    relationNames =
//        new TreeSet<String>(PlannerUtil.getRelationLineageWithinQueryBlock(plan, inner));
//    outerEdgeKey = TUtil.collectionToString(relationNames, ", ");
//    for (String outerName : PlannerUtil.getRelationLineageWithinQueryBlock(plan, outer)) {
//      if (graph.hasEdge(outerEdgeKey, outerName)) {
//        JoinEdge existJoinEdge = graph.getEdge(outerEdgeKey, outerName);
//        String[] joinEdgePair = {outerEdgeKey, outerName};
//        joinEdgePairs.add(joinEdgePair);
//        if (foundJoinEdge == null) {
//          foundJoinEdge = new JoinEdge(existJoinEdge.getJoinType(), inner, outer,
//              existJoinEdge.getJoinQual());
//        } else {
//          foundJoinEdge.addJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
//              existJoinEdge.getJoinQual()));
//        }
//      }
//    }
//    if (foundJoinEdge != null) {
//      return foundJoinEdge;
//    }
//
//    for (String outerName : PlannerUtil.getRelationLineageWithinQueryBlock(plan, outer)) {
//      for (String innerName : PlannerUtil.getRelationLineageWithinQueryBlock(plan, inner)) {
//
//        // Find all joins between two relations and merge them into one join if possible
//        if (graph.hasEdge(outerName, innerName)) {
//          JoinEdge existJoinEdge = graph.getEdge(outerName, innerName);
//          String[] joinEdgePair = {outerName, innerName};
//          joinEdgePairs.add(joinEdgePair);
//          if (foundJoinEdge == null) {
//            foundJoinEdge = new JoinEdge(existJoinEdge.getJoinType(), outer, inner,
//                existJoinEdge.getJoinQual());
//          } else {
//            foundJoinEdge.addJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
//                existJoinEdge.getJoinQual()));
//          }
//        }
//      }
//    }
//
//    if (foundJoinEdge == null) {
//      foundJoinEdge = new JoinEdge(JoinType.CROSS, outer, inner);
//    }

    return foundJoinEdge;
  }

  /**
   * Getting a cost of one join
   * @param joinEdge
   * @return
   */
  public static double getCost(JoinEdge joinEdge) {
    double filterFactor = 1;
    if (joinEdge.hasJoinQual()) {
      // TODO - should consider join type
      // TODO - should statistic information obtained from query history
      filterFactor = filterFactor * Math.pow(DEFAULT_SELECTION_FACTOR, joinEdge.getJoinQual().length);
      return getCost(joinEdge.getLeftVertex().getCorrespondingNode())
          * getCost(joinEdge.getRightVertex().getCorrespondingNode()) * filterFactor;
    } else {
      // make cost bigger if cross join
      return Math.pow(getCost(joinEdge.getLeftVertex().getCorrespondingNode())
          * getCost(joinEdge.getRightVertex().getCorrespondingNode()), 2);
    }
  }

  // TODO - costs of other operator operators (e.g., group-by and sort) should be computed in proper manners.
  public static double getCost(LogicalNode node) {
    switch (node.getType()) {

    case PROJECTION:
      ProjectionNode projectionNode = (ProjectionNode) node;
      return getCost(projectionNode.getChild());

    case JOIN:
      JoinNode joinNode = (JoinNode) node;
      double filterFactor = 1;
      if (joinNode.hasJoinQual()) {
        filterFactor = Math.pow(DEFAULT_SELECTION_FACTOR,
            AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual()).length);
        return getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild()) * filterFactor;
      } else {
        return Math.pow(getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild()), 2);
      }

    case SELECTION:
      SelectionNode selectionNode = (SelectionNode) node;
      return getCost(selectionNode.getChild()) *
          Math.pow(DEFAULT_SELECTION_FACTOR, AlgebraicUtil.toConjunctiveNormalFormArray(selectionNode.getQual()).length);

    case TABLE_SUBQUERY:
      TableSubQueryNode subQueryNode = (TableSubQueryNode) node;
      return getCost(subQueryNode.getSubQuery());

    case SCAN:
      ScanNode scanNode = (ScanNode) node;
      if (scanNode.getTableDesc().getStats() != null) {
        double cost = ((ScanNode)node).getTableDesc().getStats().getNumBytes();
        return cost;
      } else {
        return Long.MAX_VALUE;
      }

    case UNION:
      UnionNode unionNode = (UnionNode) node;
      return getCost(unionNode.getLeftChild()) + getCost(unionNode.getRightChild());

    case EXCEPT:
    case INTERSECT:
      throw new UnsupportedOperationException("getCost() does not support EXCEPT or INTERSECT yet");

    default:
      // all binary operators (join, union, except, and intersect) are handled in the above cases.
      // So, we need to handle only unary nodes in default.
      return getCost(((UnaryNode) node).getChild());
    }
  }
}