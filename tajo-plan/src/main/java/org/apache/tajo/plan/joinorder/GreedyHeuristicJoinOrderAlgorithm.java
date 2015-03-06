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

import org.apache.tajo.algebra.JoinType;
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
  public FoundJoinOrder findBestOrder(LogicalPlan plan, LogicalPlan.QueryBlock block, JoinGraph joinGraph)
      throws PlanningException {

    // Setup a remain relation set to be joined
    // Why we should use LinkedHashSet? - it should keep the deterministic for the order of joins.
    // Otherwise, join orders can be different even if join costs are the same to each other.
    List<LogicalNode> remainRelations = TUtil.newList();
    for (RelationNode relation : block.getRelations()) {
      remainRelations.add(relation);
    }

    LogicalNode latestJoin;
    JoinEdge bestPair;
    List<LogicalNode> joinInputs = TUtil.newList();

    while (remainRelations.size() > 1) {
      // Find the best join pair among all joinable operators in candidate set.
      bestPair = getBestPair(plan, joinGraph, remainRelations, joinInputs);

//      remainRelations.remove(bestPair.getLeftRelation()); // remainRels = remainRels \ Ti
//      remainRelations.remove(bestPair.getRightRelation()); // remainRels = remainRels \ Tj
      remainRelations.removeAll(joinInputs);

      latestJoin = createJoinNode(plan, bestPair, joinInputs);
      remainRelations.add(0, latestJoin);

      // all logical nodes should be registered to corresponding blocks
      block.registerNode(latestJoin);
    }

    JoinNode joinTree = (JoinNode) remainRelations.iterator().next();
    // all generated nodes should be registered to corresponding blocks
    block.registerNode(joinTree);
    return new FoundJoinOrder(joinTree, getCost(joinTree));

//    // Setup a remain relation set to be joined
//    // Why we should use LinkedHashSet? - it should keep the deterministic for the order of joins.
//    // Otherwise, join orders can be different even if join costs are the same to each other.
//
//    // to allow that the same relation is used twice or more, we must use List.
//    List<LogicalNode> remainRelations = TUtil.newList();
//    for (RelationNode relationNode : block.getRelations()) {
//      remainRelations.add(relationNode);
//    }
//
//    LogicalNode latestJoin;
//    JoinEdge bestPair;
//
//    while (remainRelations.size() > 1) {
//
//      // find the largest associative group from left
//      List<LogicalNode> associativeGroup = TUtil.newList();
//      JoinEdge lastNonAssociativePair = null;
//      for (LogicalNode candidate : remainRelations) {
//        if (associativeGroup.size() == 0) {
//          associativeGroup.add(candidate);
//          continue;
//        }
//
////        LogicalNode lastAssociativeRelation = associativeGroup.get(associativeGroup.size()-1);
//        String leftRelationLineageName = getRelationLineageName(plan, associativeGroup);
//        String rightRelationLineageName = getRelationLineageName(plan, candidate);
//
////        JoinEdge joinEdge = joinGraph.getEdge(leftRelationLineageName, rightRelationLineageName);
//        JoinEdge joinEdge = null;
//        // TODO: if the join edge is null?
//        if (!isAssociative(joinEdge)) {
//          lastNonAssociativePair = joinEdge;
//          break;
//        }
//        associativeGroup.add(candidate);
//      }
//
//      // if the associative group is found
//      if (associativeGroup.size() > 1) {
//        // find the best order within that group
//        while (associativeGroup.size() > 1) {
//          bestPair = getBestPair(plan, joinGraph, associativeGroup);
//          remainRelations.remove(bestPair.getLeftRelation());
//          remainRelations.remove(bestPair.getRightRelation());
//          associativeGroup.remove(bestPair.getLeftRelation());
//          associativeGroup.remove(bestPair.getRightRelation());
//          latestJoin = createJoinNode(plan, bestPair);
//          associativeGroup.add(latestJoin);
//          block.registerNode(latestJoin);
//        }
//
//        // add the group to the remaining relation
//        remainRelations.add(0, associativeGroup.get(0));
//      } else {
//        if (lastNonAssociativePair == null) {
//          throw new PlanningException("Cannot find any join relationships");
//        }
//        // drop the current relation and move the cursor to right
//        // NOTICE: dropped relations are freezed, and their order must not be changed
//        remainRelations.remove(lastNonAssociativePair.getLeftRelation());
//        remainRelations.remove(lastNonAssociativePair.getRightRelation());
//        latestJoin = createJoinNode(plan, lastNonAssociativePair);
//        block.registerNode(latestJoin);
//        remainRelations.add(0, latestJoin);
//      }
//    }
//
//    JoinNode joinTree = (JoinNode) remainRelations.iterator().next();
//    // all generated nodes should be registered to corresponding blocks
//    block.registerNode(joinTree);
//    return new FoundJoinOrder(joinTree, getCost(joinTree));
  }

  private static String getRelationLineageName(LogicalPlan plan, List<LogicalNode> nodes) throws PlanningException {
    StringBuilder sb = new StringBuilder();
    for (LogicalNode node : nodes) {
      sb.append(getRelationLineageName(plan, node)).append(",");
    }
    sb.deleteCharAt(sb.length()-1);
    return sb.toString();
  }

  private static String getRelationLineageName(LogicalPlan plan, LogicalNode node) throws PlanningException {
    return TUtil.collectionToString(PlannerUtil.getRelationNamesLineageWithinQueryBlock(plan, node), ",");
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
  private static boolean isAssociative(JoinEdge joinEdge) {
    switch(joinEdge.getJoinType()) {
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

  private static JoinNode createJoinNode(LogicalPlan plan, JoinEdge joinEdge, List<LogicalNode> actualChilds) {
//    LogicalNode left = joinEdge.getLeftRelation();
//    LogicalNode right = joinEdge.getRightRelation();
    LogicalNode left = actualChilds.get(0);
    LogicalNode right = actualChilds.get(1);

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
//    joinNode.setTargets(joinEdge.getTargets());
    return joinNode;
  }

  /**
   * Find the best join pair among all joinable operators in candidate set.
   *
   * @param plan a logical plan
   * @param graph a join graph which consists of vertices and edges, where vertex is relation and
   *              each edge is join condition.
   * @param candidateSet candidate operators to be joined.
   * @return The best join pair among them
   * @throws PlanningException
   */
  private JoinEdge getBestPair(LogicalPlan plan, JoinGraph graph, List<LogicalNode> candidateSet,
                               List<LogicalNode> willBeRemoved)
      throws PlanningException {
    double minCost = Double.MAX_VALUE;
    JoinEdge bestJoin = null;

    double minNonCrossJoinCost = Double.MAX_VALUE;
    JoinEdge bestNonCrossJoin = null;

    for (LogicalNode outer : candidateSet) {
      for (LogicalNode inner : candidateSet) {
        if (outer.equals(inner)) {
          continue;
        }

        JoinEdge foundJoin = findJoin(plan, graph, outer, inner);
        if (foundJoin == null) {
          continue;
        }
        double cost = getCost(foundJoin);

        if (cost < minCost) {
          minCost = cost;
          bestJoin = foundJoin;
          willBeRemoved.clear();
          willBeRemoved.add(outer);
          willBeRemoved.add(inner);
        }

        // Keep the min cost join
        // But, if there exists a qualified join, the qualified join must be chosen
        // rather than cross join regardless of cost.
        if (foundJoin.hasJoinQual()) {
          if (cost < minNonCrossJoinCost) {
            minNonCrossJoinCost = cost;
            bestNonCrossJoin = foundJoin;
            willBeRemoved.clear();
            willBeRemoved.add(outer);
            willBeRemoved.add(inner);
          }
        }
      }
    }

    if (bestNonCrossJoin != null) {
      return bestNonCrossJoin;
    } else {
      return bestJoin;
    }
  }

  /**
   * Find a join between two logical operator trees
   *
   * @return If there is no join condition between two relation, it returns NULL value.
   */
  private static JoinEdge findJoin(LogicalPlan plan, JoinGraph graph, LogicalNode outer, LogicalNode inner)
      throws PlanningException {
    JoinEdge foundJoinEdge = null;

    RelationNode leftRelation, rightRelation;
    if (outer instanceof RelationNode) {
      leftRelation = (RelationNode) outer;
    } else {
      leftRelation = PlannerUtil.getMostRightRelNameWithinLineage(plan, outer);
    }
    if (inner instanceof RelationNode) {
      rightRelation = (RelationNode) inner;
    } else {
      rightRelation = PlannerUtil.getMostLeftRelNameWithinLineage(plan, inner);
    }

    return graph.getEdge(leftRelation.getCanonicalName(), rightRelation.getCanonicalName());

//    // If outer is outer join, make edge key using all relation names in outer.
////    Collection<String> relationNames =
////        PlannerUtil.getRelationNamesLineageWithinQueryBlock(plan, outer);
////    String outerEdgeKey = TUtil.collectionToString(relationNames, ", ");
//    String outerEdgeKey = getRelationLineageName(plan, outer);
//    for (String innerName : PlannerUtil.getRelationNamesLineageWithinQueryBlock(plan, inner)) {
//      if (graph.hasEdge(outerEdgeKey, innerName)) {
//        JoinEdge existJoinEdge = graph.getEdge(outerEdgeKey, innerName);
//        if (foundJoinEdge == null) {
////          foundJoinEdge = new JoinEdge(existJoinEdge.getJoinType(), outer, inner,
////              existJoinEdge.getJoinQual());
//          foundJoinEdge = new JoinEdge(existJoinEdge.getJoinNode());
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
////    relationNames =
////        PlannerUtil.getRelationNamesLineageWithinQueryBlock(plan, inner);
////    outerEdgeKey = TUtil.collectionToString(relationNames, ", ");
//    outerEdgeKey = getRelationLineageName(plan, inner);
//    for (String outerName : PlannerUtil.getRelationNamesLineageWithinQueryBlock(plan, outer)) {
////      if (graph.hasEdge(outerEdgeKey, outerName)) {
////        JoinEdge existJoinEdge = graph.getEdge(outerEdgeKey, outerName);
////        if (foundJoinEdge == null) {
//////          foundJoinEdge = new JoinEdge(existJoinEdge.getJoinType(), inner, outer,
//////              existJoinEdge.getJoinQual());
////          foundJoinEdge = new JoinEdge(existJoinEdge.getJoinNode());
////        } else {
////          foundJoinEdge.addJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
////              existJoinEdge.getJoinQual()));
////        }
////      }
//    }
//    if (foundJoinEdge != null) {
//      return foundJoinEdge;
//    }
//
//    for (String outerName : PlannerUtil.getRelationNamesLineageWithinQueryBlock(plan, outer)) {
//      for (String innerName : PlannerUtil.getRelationNamesLineageWithinQueryBlock(plan, inner)) {
//
//        // Find all joins between two relations and merge them into one join if possible
////        if (graph.hasEdge(outerName, innerName)) {
////          JoinEdge existJoinEdge = graph.getEdge(outerName, innerName);
////          if (foundJoinEdge == null) {
//////            foundJoinEdge = new JoinEdge(existJoinEdge.getJoinType(), outer, inner,
//////                existJoinEdge.getJoinQual());
////            foundJoinEdge = new JoinEdge(existJoinEdge.getJoinNode());
////          } else {
////            foundJoinEdge.addJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
////                existJoinEdge.getJoinQual()));
////          }
////        }
//      }
//    }
//
//    if (foundJoinEdge == null) {
////      foundJoinEdge = new JoinEdge(JoinType.CROSS, outer, inner);
//      JoinNode cross = plan.createNode(JoinNode.class);
//      cross.init(JoinType.CROSS, outer, inner);
//      Schema mergedSchema = SchemaUtil.merge(outer.getOutSchema(),
//          inner.getOutSchema());
//      cross.setInSchema(mergedSchema);
//      cross.setOutSchema(mergedSchema);
//      cross.setTargets(PlannerUtil.schemaToTargets(mergedSchema));
//
////      foundJoinEdge = new JoinEdge(cross);
//    }

//    return foundJoinEdge;
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
      return getCost(joinEdge.getLeftRelation()) * getCost(joinEdge.getRightRelation()) * filterFactor;
    } else {
      // make cost bigger if cross join
      return Math.pow(getCost(joinEdge.getLeftRelation()) * getCost(joinEdge.getRightRelation()), 2);
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