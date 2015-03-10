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
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.util.TUtil;

import java.util.Map;
import java.util.Set;

public class AssociativeGroup implements JoinVertex {

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

  @Override
  public Schema getSchema() {
    return vertex == null ? null : vertex.getSchema();
  }

  @Override
  public LogicalNode getCorrespondingNode() {
    return vertex == null ? null : vertex.getCorrespondingNode();
  }

  public int size() {
    return edgeNum;
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
//  private static boolean checkIfBeEvaluatedAtJoin(EvalNode evalNode, JoinVertex left, JoinVertex right) {
//    Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);
//
//    if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
//      return false;
//    }
//
//    if (EvalTreeUtil.findEvalsByType(evalNode, EvalType.WINDOW_FUNCTION).size() > 0) {
//      return false;
//    }
//
//    Schema merged = SchemaUtil.merge(left.getSchema(), right.getSchema());
//    if (columnRefs.size() > 0 && !merged.containsAll(columnRefs)) {
//      return false;
//    }
//
//    return true;
//  }
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