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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class AssociativeGroupVertex implements JoinVertex {
  private Set<JoinVertex> vertexes = TUtil.newHashSet();
//  private Map<VertexPair, JoinEdge> joinEdges = TUtil.newHashMap();
  private Set<EvalNode> predicates = TUtil.newHashSet();
  private JoinNode joinNode; // corresponding join node

  public void addVertex(JoinVertex vertex) {
    this.vertexes.add(vertex);
  }

  public void addPredicate(EvalNode predicate) {
    this.predicates.add(predicate);
  }

  public void addPredicates(Set<EvalNode> predicates) {
    this.predicates.addAll(predicates);
  }

  public void removeVertex(JoinVertex vertex) {
    this.vertexes.remove(vertex);
  }

  public void removePredicates(Set<EvalNode> predicates) {
    this.predicates.removeAll(predicates);
  }

//  public void addJoinEdge(JoinVertex left, JoinVertex right, JoinEdge edge) {
//    this.joinEdges.put(new VertexPair(left, right), edge);
//  }
//
//  public void addJoinEdge(JoinEdge joinEdge) {
////    addVertex(joinEdge.getLeftVertex());
////    addVertex(joinEdge.getRightVertex());
//    this.addJoinEdge(joinEdge.getLeftVertex(), joinEdge.getRightVertex(), joinEdge);
//  }

  public boolean isEmpty() {
    return vertexes.isEmpty();
  }

  public int size() {
    return vertexes.size();
  }

  public Set<JoinVertex> getVertexes() {
    return vertexes;
  }

  public Set<EvalNode> getPredicates() {
    return predicates;
  }

//  public Map<VertexPair, JoinEdge> getJoinEdgeMap() {
//    return joinEdges;
//  }
//
//  public Collection<JoinEdge> getJoinEdges() {
//    return joinEdges.values();
//  }

  public Collection<JoinEdge> getJoinEdges() {
    return populateVertexPairs().values();
  }

  public Map<VertexPair, JoinEdge> populateVertexPairs() {

    // get or create join nodes for every vertex pairs
    Map<VertexPair, JoinEdge> populatedJoins = TUtil.newHashMap();
    VertexPair keyVertexPair;

    for (JoinVertex left : vertexes) {
      for (JoinVertex right : vertexes) {
        if (left.equals(right)) continue;
        keyVertexPair = new VertexPair(left, right);
        JoinEdge joinEdge = createJoinEdge(JoinType.CROSS, left, right);
        // join conditions must be referred to decide the join type between INNER and CROSS.
        // In addition, some join conditions can be moved to the optimal places due to the changed join order
        Set<EvalNode> conditionsForThisJoin = TUtil.newHashSet();
        for (EvalNode predicate : predicates) {
          if (EvalTreeUtil.isJoinQual(null, left.getSchema(), right.getSchema(), predicate, false)
              && checkIfBeEvaluatedAtJoin(predicate, left, right)) {
            conditionsForThisJoin.add(predicate);
          }
        }
        if (!conditionsForThisJoin.isEmpty()) {
          joinEdge.setJoinType(JoinType.INNER);
          joinEdge.addJoinQuals(conditionsForThisJoin);
        }

        populatedJoins.put(keyVertexPair, joinEdge);
      }
    }
    return populatedJoins;
  }

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

  private static JoinEdge createJoinEdge(JoinType joinType, JoinVertex leftVertex, JoinVertex rightVertex) {
    return new JoinEdge(joinType, leftVertex, rightVertex);
  }

  @Override
  public String toString() {
    return "V (" + TUtil.collectionToString(vertexes, ",") + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof AssociativeGroupVertex) {
      AssociativeGroupVertex other = (AssociativeGroupVertex) o;
      return TUtil.checkEquals(this.vertexes, other.vertexes);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return vertexes.hashCode();
  }

  @Override
  public Schema getSchema() {
    Schema schema = new Schema();
    for (JoinVertex v : vertexes) {
      schema = SchemaUtil.merge(schema, v.getSchema());
    }
    return schema;
  }

  @Override
  public LogicalNode getCorrespondingNode() {
    return joinNode;
  }

  public void setJoinNode(JoinNode joinNode) {
    this.joinNode = joinNode;
  }
}