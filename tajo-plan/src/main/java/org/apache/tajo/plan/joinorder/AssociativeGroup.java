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
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.expr.EvalType;
import org.apache.tajo.util.TUtil;

import java.util.Set;
import java.util.Stack;

public class AssociativeGroup {

  private JoinGroupVertex vertex;
  private JoinEdge mostLeftEdge;
  private JoinEdge mostRightEdge;
  private int edgeNum = 0;

  public void addVertex(JoinGroupVertex vertex) {
    // TODO: connectivity check
    this.vertex = vertex;
    if (this.mostLeftEdge == null) {
      this.mostLeftEdge = vertex.getJoinEdge();
    }
    this.mostRightEdge = vertex.getJoinEdge();
    this.edgeNum++;
  }

  public JoinType getMostRightJoinType() {
    return getMostRightEdge().getJoinType();
  }

  public JoinEdge getMostRightEdge() {
    return mostRightEdge;
  }

  public JoinEdge getMostLeftEdge() {
    return mostLeftEdge;
  }

  public void clear() {
    this.vertex = null;
    this.mostLeftEdge = mostRightEdge = null;
    this.edgeNum = 0;
  }

  public JoinVertex getVertex() {
    return this.vertex;
  }

  public int size() {
    return edgeNum;
  }

  private static class JoinPredicateCollectorContext {
    private final Set<EvalNode> predicates = TUtil.newHashSet();
  }

  private static class JoinPredicateCollector {
    public void visit(JoinPredicateCollectorContext context, Stack<JoinVertex> stack, JoinVertex vertex) {
      stack.push(vertex);
      if (vertex instanceof JoinGroupVertex) {
        JoinGroupVertex groupVertex = (JoinGroupVertex) vertex;
        visit(context, stack, groupVertex.getJoinEdge().getLeftVertex());
        visit(context, stack, groupVertex.getJoinEdge().getRightVertex());

        context.predicates.addAll(TUtil.newList(groupVertex.getJoinEdge().getJoinQual()));
      }
      stack.pop();
    }
  }

  private Set<EvalNode> collectJoinPredicates() {
    JoinPredicateCollector collector = new JoinPredicateCollector();
    JoinPredicateCollectorContext context = new JoinPredicateCollectorContext();
    collector.visit(context, new Stack<JoinVertex>(), this.getVertex());
    return context.predicates;
  }

  private static class JoinVertexFinderContext {
    boolean findMostLeft = false;
    boolean findMostRight = false;
    JoinVertex foundVertex;
  }

  private static class JoinVertexFinder {

    public JoinVertex findMostRightRelationVertex(JoinVertex vertex) {
      JoinVertexFinderContext context = new JoinVertexFinderContext();
      context.findMostRight = true;
      visit(context, new Stack<JoinVertex>(), vertex);
      return context.foundVertex;
    }

    private void visit(JoinVertexFinderContext context, Stack<JoinVertex> stack, JoinVertex vertex) {
      stack.push(vertex);
      if (vertex instanceof JoinGroupVertex) {
        JoinGroupVertex groupVertex = (JoinGroupVertex) vertex;
        visit(context, stack, groupVertex.getJoinEdge().getRightVertex());
      } else if (vertex instanceof RelationVertex) {
        context.foundVertex = vertex;
      }
      stack.pop();
    }
  }

  private static class JoinEdgeCollectorContext {
    private final Set<JoinEdge> joinEdges = TUtil.newHashSet();
    private final Set<EvalNode> predicates;

    public JoinEdgeCollectorContext(Set<EvalNode> predicates) {
      this.predicates = predicates;
    }
  }

  private static class JoinEdgeCollector {
    public void visit(JoinEdgeCollectorContext context, Stack<JoinVertex> stack, JoinVertex vertex) {
      stack.push(vertex);
      if (vertex instanceof JoinGroupVertex) {
        JoinGroupVertex groupVertex = (JoinGroupVertex) vertex;
        visit(context, stack, groupVertex.getJoinEdge().getLeftVertex());
        visit(context, stack, groupVertex.getJoinEdge().getRightVertex());

        // original join edge
        JoinEdge edge = groupVertex.getJoinEdge();
        // join conditions must be referred to decide the join type between INNER and CROSS.
        // In addition, some join conditions can be moved to the optimal places due to the changed join order
        Set<EvalNode> conditionsForThisJoin = findConditionsForJoin(context.predicates, edge);
        if (!conditionsForThisJoin.isEmpty()) {
          if (edge.getJoinType() == JoinType.CROSS) {
            edge.setJoinType(JoinType.INNER);
          }
          edge.setJoinQuals(conditionsForThisJoin);
        }

        // new join edge according to the associative rule
        JoinVertexFinder finder = new JoinVertexFinder();
        JoinVertex mostRightRelationFromLeftChild = finder.findMostRightRelationVertex(vertex);
        JoinEdge newEdge = new JoinEdge(groupVertex.getJoinType(), mostRightRelationFromLeftChild,
            groupVertex.getJoinEdge().getRightVertex());

        conditionsForThisJoin.clear();
        conditionsForThisJoin = findConditionsForJoin(context.predicates, newEdge);
        if (!conditionsForThisJoin.isEmpty()) {
          if (newEdge.getJoinType() == JoinType.CROSS) {
            newEdge.setJoinType(JoinType.INNER);
          }
          newEdge.setJoinQuals(conditionsForThisJoin);
        }
      }
      stack.pop();
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

  public Set<JoinEdge> populateJoinEdges() {
    // collect join predicates within the group
    Set<EvalNode> predicates = collectJoinPredicates();

    JoinEdgeCollectorContext context = new JoinEdgeCollectorContext(predicates);
    JoinEdgeCollector collector = new JoinEdgeCollector();
    collector.visit(context, new Stack<JoinVertex>(), this.vertex);
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