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

package org.apache.tajo.plan.joinorder;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.expr.EvalType;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.RelationNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.TUtil;

import java.util.Set;
import java.util.Stack;

public class AssociativeGroup {

  private JoinGroupVertex mostLeftVertex;
  private JoinGroupVertex mostRightVertex;
  private int edgeNum = 0;

  public void addVertex(JoinGroupVertex vertex) {
    // TODO: connectivity check
    if (this.mostLeftVertex == null) {
      this.mostLeftVertex = vertex;
    }
    this.mostRightVertex = vertex;
    this.edgeNum++;
  }

  public JoinType getMostRightJoinType() {
    return mostRightVertex.getJoinType();
  }

  public void clear() {
    this.mostLeftVertex = mostRightVertex = null;
    this.edgeNum = 0;
  }

  public JoinGroupVertex getMostLeftVertex() {
    return this.mostLeftVertex;
  }

  public JoinGroupVertex getMostRightVertex() {
    return this.mostRightVertex;
  }

  public int size() {
    return edgeNum;
  }

  private static class JoinPredicateCollectorContext {
    private final Set<EvalNode> predicates = TUtil.newHashSet();
  }

  private static class JoinPredicateCollector extends JoinTreeVisitor<JoinPredicateCollectorContext> {

    @Override
    public void visitJoinGroupVertex(JoinPredicateCollectorContext context, Stack<JoinVertex> stack,
                                     JoinGroupVertex vertex) {
      super.visitJoinGroupVertex(context, stack, vertex);
      context.predicates.addAll(TUtil.newList(vertex.getJoinEdge().getJoinQual()));
    }
  }

  private Set<EvalNode> collectJoinPredicates() {
    JoinPredicateCollector collector = new JoinPredicateCollector();
    JoinPredicateCollectorContext context = new JoinPredicateCollectorContext();
    collector.visit(context, new Stack<JoinVertex>(), this.getMostRightVertex());
    return context.predicates;
  }

  private static class JoinEdgeCollectorContext {
    private final LogicalPlan plan;
    private final Set<JoinEdge> joinEdges = TUtil.newHashSet();
    private final Set<EvalNode> predicates;
    private final JoinGroupVertex mostLeftVertex;
    private final JoinGroupVertex mostRightVertex;

    public JoinEdgeCollectorContext(LogicalPlan plan, JoinGroupVertex mostLeftVertex, JoinGroupVertex mostRightVertex,
                                    Set<EvalNode> predicates) {
      this.plan = plan;
      this.mostLeftVertex = mostLeftVertex;
      this.mostRightVertex = mostRightVertex;
      this.predicates = predicates;
    }
  }

  private static class JoinEdgeCollector extends JoinTreeVisitor<JoinEdgeCollectorContext> {
//    public void visit(JoinEdgeCollectorContext context, Stack<JoinVertex> stack, JoinVertex vertex) {
//      stack.push(vertex);
//      if (vertex instanceof JoinGroupVertex) {
//        JoinGroupVertex groupVertex = (JoinGroupVertex) vertex;
//        if (!groupVertex.getJoinEdge().getLeftVertex().equals(context.mostLeftVertex)) {
//          visit(context, stack, groupVertex.getJoinEdge().getLeftVertex());
//        }
//        if (!groupVertex.getJoinEdge().getRightVertex().equals(context.mostRightVertex)) {
//          visit(context, stack, groupVertex.getJoinEdge().getRightVertex());
//        }
//
//        // original join edge
//        JoinEdge edge = groupVertex.getJoinEdge();
//        // join conditions must be referred to decide the join type between INNER and CROSS.
//        // In addition, some join conditions can be moved to the optimal places due to the changed join order
//        Set<EvalNode> conditionsForThisJoin = findConditionsForJoin(context.predicates, edge);
//        if (!conditionsForThisJoin.isEmpty()) {
//          if (edge.getJoinType() == JoinType.CROSS) {
//            edge.setJoinType(JoinType.INNER);
//          }
//          edge.setJoinQuals(conditionsForThisJoin);
//        }
//        context.joinEdges.add(edge);
//
//        // TODO: how to keep nodes of other types?
//        // new join edge according to the associative rule
//        RelationVertexFinder finder = new RelationVertexFinder();
//        JoinVertex mostRightRelationFromLeftChild = finder.findMostRightRelationVertex(
//            groupVertex.getJoinEdge().getLeftVertex()).iterator().next();
//        JoinEdge newEdge = new JoinEdge(groupVertex.getJoinType(), mostRightRelationFromLeftChild,
//            groupVertex.getJoinEdge().getRightVertex());
//
//        conditionsForThisJoin.clear();
//        conditionsForThisJoin = findConditionsForJoin(context.predicates, newEdge);
//        if (!conditionsForThisJoin.isEmpty()) {
//          if (newEdge.getJoinType() == JoinType.CROSS) {
//            newEdge.setJoinType(JoinType.INNER);
//          }
//          newEdge.setJoinQuals(conditionsForThisJoin);
//        }
//
//        JoinEdge newJoinEdgeInfo = ((JoinGroupVertex)edge.getLeftVertex()).getJoinEdge();
//        JoinGroupVertex newVertex = new JoinGroupVertex(newEdge);
//        newVertex.setJoinNode(createJoinNode(context.plan, newEdge));
//        newEdge = new JoinEdge(newJoinEdgeInfo.getJoinType(),
//            newJoinEdgeInfo.getLeftVertex(), newVertex);
//        conditionsForThisJoin.clear();
//        conditionsForThisJoin = findConditionsForJoin(context.predicates, newEdge);
//        if (!conditionsForThisJoin.isEmpty()) {
//          if (newEdge.getJoinType() == JoinType.CROSS) {
//            newEdge.setJoinType(JoinType.INNER);
//          }
//          newEdge.setJoinQuals(conditionsForThisJoin);
//        }
//
//        context.joinEdges.add(newEdge);
//      }
//      stack.pop();
//    }

    @Override
    public void visitJoinGroupVertex(JoinEdgeCollectorContext context, Stack<JoinVertex> stack, JoinGroupVertex vertex) {
      if (!vertex.getJoinEdge().getLeftVertex().equals(context.mostLeftVertex)) {
        visit(context, stack, vertex.getJoinEdge().getLeftVertex());
      }
      if (!vertex.getJoinEdge().getRightVertex().equals(context.mostRightVertex)) {
        visit(context, stack, vertex.getJoinEdge().getRightVertex());
      }

      // original join edge
      JoinEdge edge = vertex.getJoinEdge();
      // join conditions must be referred to decide the join type between INNER and CROSS.
      // In addition, some join conditions can be moved to the optimal places due to the changed join order
      Set<EvalNode> conditionsForThisJoin = findConditionsForJoin(context.predicates, edge);
      if (!conditionsForThisJoin.isEmpty()) {
        if (edge.getJoinType() == JoinType.CROSS) {
          edge.setJoinType(JoinType.INNER);
        }
        edge.setJoinQuals(conditionsForThisJoin);
      }
      context.joinEdges.add(edge);
      if (PlannerUtil.isCommutativeJoin(edge.getJoinType())) {
        context.joinEdges.add(new JoinEdge(edge.getJoinType(), edge.getRightVertex(), edge.getLeftVertex()));
      }

      // TODO: how to keep nodes of other types?
      // new join edge according to the associative rule
      JoinVertex mostRightRelationFromLeftChild = JoinOrderUtil.findMostRightRelationVertex(
          vertex.getJoinEdge().getLeftVertex());
      Set<JoinEdge> childEdges = TUtil.newHashSet();
      JoinEdge childEdge = new JoinEdge(vertex.getJoinType(), mostRightRelationFromLeftChild,
          vertex.getJoinEdge().getRightVertex());
      childEdges.add(childEdge);
      if (PlannerUtil.isCommutativeJoin(childEdge.getJoinType())) {
        childEdges.add(new JoinEdge(childEdge.getJoinType(), childEdge.getRightVertex(), childEdge.getLeftVertex()));
      }

      conditionsForThisJoin.clear();
      conditionsForThisJoin = findConditionsForJoin(context.predicates, childEdge);
      if (!conditionsForThisJoin.isEmpty()) {
        for (JoinEdge eachChild : childEdges) {
          if (eachChild.getJoinType() == JoinType.CROSS) {
            eachChild.setJoinType(JoinType.INNER);
          }
          eachChild.setJoinQuals(conditionsForThisJoin);
        }
      }

      Set<JoinEdge> newEdges = TUtil.newHashSet();
      JoinEdge newJoinEdgeInfo = ((JoinGroupVertex)edge.getLeftVertex()).getJoinEdge();
      JoinGroupVertex newVertex = new JoinGroupVertex(childEdge);
      newVertex.setJoinNode(createJoinNode(context.plan, childEdge));
      JoinEdge newEdge = new JoinEdge(newJoinEdgeInfo.getJoinType(),
          newJoinEdgeInfo.getLeftVertex(), newVertex);
      newEdges.add(newEdge);
      if (PlannerUtil.isCommutativeJoin(newEdge.getJoinType())) {
        newEdges.add(new JoinEdge(newEdge.getJoinType(), newEdge.getRightVertex(), newEdge.getLeftVertex()));
      }
      conditionsForThisJoin.clear();
      conditionsForThisJoin = findConditionsForJoin(context.predicates, newEdge);
      if (!conditionsForThisJoin.isEmpty()) {
        for (JoinEdge eachEdge : newEdges) {
          if (eachEdge.getJoinType() == JoinType.CROSS) {
            eachEdge.setJoinType(JoinType.INNER);
          }
          eachEdge.setJoinQuals(conditionsForThisJoin);
        }
      }

      context.joinEdges.addAll(newEdges);
    }

    private Set<EvalNode> findConditionsForJoin(Set<EvalNode> candidates, JoinEdge edge) {
      Set<EvalNode> conditionsForThisJoin = TUtil.newHashSet();
      for (EvalNode predicate : candidates) {
        if (EvalTreeUtil.isJoinQual(null, edge.getLeftVertex().getSchema(),
            edge.getRightVertex().getSchema(), predicate, false)
            && checkIfBeEvaluatedAtJoin(predicate, edge.getLeftVertex(), edge.getRightVertex())) {
          conditionsForThisJoin.add(predicate);
        }
      }
      return conditionsForThisJoin;
    }
  }

  private static JoinNode createJoinNode(LogicalPlan plan, JoinEdge joinEdge) {
    LogicalNode left = joinEdge.getLeftVertex().getCorrespondingNode();
    LogicalNode right = joinEdge.getRightVertex().getCorrespondingNode();

    JoinNode joinNode = plan.createNode(JoinNode.class);

    // TODO: move to the propoer position
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

  public Set<JoinEdge> populateJoinEdges(LogicalPlan plan) {
    // collect join predicates within the group
    Set<EvalNode> predicates = collectJoinPredicates();

    JoinEdgeCollectorContext context = new JoinEdgeCollectorContext(plan, mostLeftVertex,
        mostRightVertex, predicates);
    JoinEdgeCollector collector = new JoinEdgeCollector();
    collector.visit(context, new Stack<JoinVertex>(), this.mostRightVertex);
    return context.joinEdges;
  }

//  private Set<JoinVertex> vertexes = TUtil.newHashSet();
//  private Map<VertexPair, JoinEdge> joinEdges = TUtil.newHashMap();
//  private Set<EvalNode> predicateCandiates = TUtil.newHashSet();
//  private JoinNode joinNode; // corresponding join node
//
//  private JoinEdge mostLeftEdge;
//  private JoinEdge mostRightEdge;
//
////  public void addVertex(JoinVertex vertex) {
////    this.vertexes.add(vertex);
////  }
//
//  public void addPredicates(Set<EvalNode> predicates) {
//    this.predicateCandiates.addAll(predicates);
//  }
//
////  public void removeVertex(JoinVertex vertex) {
////    this.vertexes.remove(vertex);
////  }
//
//  public void removePredicates(Set<EvalNode> predicates) {
//    this.predicateCandiates.removeAll(predicates);
//  }
//
////  public void addJoinEdge(JoinVertex left, JoinVertex right, JoinEdge edge) {
////    this.joinEdges.put(new VertexPair(left, right), edge);
////  }
//
//  public void addJoinEdge(JoinEdge joinEdge) {
////    addVertex(joinEdge.getLeftVertex());
////    addVertex(joinEdge.getRightVertex());
//    // TODO: connection check
//    this.joinEdges.put(new VertexPair(joinEdge.getLeftVertex(), joinEdge.getRightVertex()), joinEdge);
//  }
//
//  public void removeJoinEdge(JoinEdge joinEdge) {
//    // TODO
//  }
//
//  public boolean isEmpty() {
//    return vertexes.isEmpty();
//  }
//
//  public int size() {
//    return vertexes.size();
//  }
//
//  public Set<JoinVertex> getVertexes() {
//    return vertexes;
//  }
//
//  public Set<EvalNode> getPredicates() {
//    return predicateCandiates;
//  }
//
//  public Set<JoinEdge> getJoinEdges() {
////    this.joinEdges.addAll(populateVertexPairs());
//    return populateVertexPairs();
//  }
//
//  public Set<JoinEdge> populateVertexPairs() {
//
//    // get or create join nodes for every vertex pairs
//    Set<JoinEdge> populatedJoins = TUtil.newHashSet();
//    VertexPair keyVertexPair;
//
//    // TODO: commutative type check
//    for (JoinVertex left : vertexes) {
//      for (JoinVertex right : vertexes) {
//        if (left.equals(right)) continue;
//        keyVertexPair = new VertexPair(left, right);
//        JoinEdge joinEdge;
//        if (joinEdges.containsKey(keyVertexPair)) {
//          joinEdge = joinEdges.get(keyVertexPair);
//        } else {
//          joinEdge =createJoinEdge(JoinType.CROSS, left, right);
//          // join conditions must be referred to decide the join type between INNER and CROSS.
//          // In addition, some join conditions can be moved to the optimal places due to the changed join order
//          Set<EvalNode> conditionsForThisJoin = TUtil.newHashSet();
//          for (EvalNode predicate : predicateCandiates) {
//            if (EvalTreeUtil.isJoinQual(null, left.getSchema(), right.getSchema(), predicate, false)
//                && checkIfBeEvaluatedAtJoin(predicate, left, right)) {
//              conditionsForThisJoin.add(predicate);
//            }
//          }
//          if (!conditionsForThisJoin.isEmpty()) {
//            joinEdge.setJoinType(JoinType.INNER);
//            joinEdge.addJoinQuals(conditionsForThisJoin);
//          }
//        }
//
//        populatedJoins.add(joinEdge);
//      }
//    }
//    return populatedJoins;
//  }
//
  private static boolean checkIfBeEvaluatedAtJoin(EvalNode evalNode, JoinVertex left, JoinVertex right) {
    Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);

    if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
      return false;
    }

    if (EvalTreeUtil.findEvalsByType(evalNode, EvalType.WINDOW_FUNCTION).size() > 0) {
      return false;
    }

    Schema merged = SchemaUtil.merge(left.getSchema(), right.getSchema());
    if (columnRefs.size() > 0 && !merged.containsAll(columnRefs)) {
      return false;
    }

    return true;
  }
//
//  private static JoinEdge createJoinEdge(JoinType joinType, JoinVertex leftVertex, JoinVertex rightVertex) {
//    return new JoinEdge(joinType, leftVertex, rightVertex);
//  }
//
//  @Override
//  public String toString() {
//    return "(" + TUtil.collectionToString(vertexes, ",") + ")";
//  }
//
//  @Override
//  public boolean equals(Object o) {
//    if (o instanceof AssociativeGroup) {
//      AssociativeGroup other = (AssociativeGroup) o;
//      return TUtil.checkEquals(this.vertexes, other.vertexes);
//    }
//    return false;
//  }
//
//  @Override
//  public int hashCode() {
//    return vertexes.hashCode();
//  }
//
//  @Override
//  public Schema getSchema() {
//    Schema schema = new Schema();
//    for (JoinVertex v : vertexes) {
//      schema = SchemaUtil.merge(schema, v.getSchema());
//    }
//    return schema;
//  }
//
//  @Override
//  public LogicalNode getCorrespondingNode() {
//    return joinNode;
//  }
//
//  public void setJoinNode(JoinNode joinNode) {
//    this.joinNode = joinNode;
//  }
}