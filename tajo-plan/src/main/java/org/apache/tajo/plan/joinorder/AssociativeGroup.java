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

    @Override
    public void visitJoinGroupVertex(JoinEdgeCollectorContext context, Stack<JoinVertex> stack, JoinGroupVertex vertex) {
      if (!vertex.equals(context.mostLeftVertex)) {
        visit(context, stack, vertex.getJoinEdge().getLeftVertex());
      }
      if (!vertex.equals(context.mostRightVertex)) {
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

      context.joinEdges.addAll(childEdges);

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
}