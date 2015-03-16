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
    private final Set<JoinEdge> totalJoinEdges = TUtil.newHashSet();
    private final Set<JoinEdge> joinEdgesFromChild = TUtil.newHashSet();
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

    private static JoinEdge getCommutativeJoinEdge(JoinEdge edge) {
      JoinEdge commutativeEdge = new JoinEdge(edge.getJoinType(), edge.getRightVertex(), edge.getLeftVertex());
      if (edge.hasJoinQual()) {
        commutativeEdge.setJoinQuals(TUtil.newHashSet(edge.getJoinQual()));
      }
      return commutativeEdge;
    }

    private static JoinEdge updateJoinQualIfNecessary(JoinEdgeCollectorContext context, JoinEdge edge) {
      // join conditions must be referred to decide the join type between INNER and CROSS.
      // In addition, some join conditions can be moved to the optimal places due to the changed join order
      Set<EvalNode> conditionsForThisJoin = findConditionsForJoin(context.predicates, edge);
      if (!conditionsForThisJoin.isEmpty()) {
        if (edge.getJoinType() == JoinType.CROSS) {
          edge.setJoinType(JoinType.INNER);
        }
        edge.setJoinQuals(conditionsForThisJoin);
      }
      return edge;
    }

    @Override
    public void visitJoinGroupVertex(JoinEdgeCollectorContext context, Stack<JoinVertex> stack, JoinGroupVertex vertex) {
      if (!vertex.equals(context.mostLeftVertex)) {
        visit(context, stack, vertex.getJoinEdge().getLeftVertex());
      }
      if (!vertex.equals(context.mostRightVertex)) {
        visit(context, stack, vertex.getJoinEdge().getRightVertex());
      }

      if (context.joinEdgesFromChild.isEmpty()) {
        // case 1: (a - b)
        context.joinEdgesFromChild.add(vertex.getJoinEdge());
        context.totalJoinEdges.add(vertex.getJoinEdge());
        if (PlannerUtil.isCommutativeJoin(vertex.getJoinEdge().getJoinType())) {
          // case 2: (b - a)
          JoinEdge commutativeEdge = getCommutativeJoinEdge(vertex.getJoinEdge());
          context.joinEdgesFromChild.add(commutativeEdge);
          context.totalJoinEdges.add(commutativeEdge);
        }
        return;
      }

      Set<JoinEdge> created = TUtil.newHashSet();
      JoinEdge thisEdge = vertex.getJoinEdge();

      // case 3: ((a - b) - c)
      JoinVertex cVertex = thisEdge.getRightVertex();
      for (JoinEdge abEdge : context.joinEdgesFromChild) {
        if (!abEdge.getLeftVertex().equals(cVertex) && !abEdge.getRightVertex().equals(cVertex)) {
          JoinGroupVertex abVertex = new JoinGroupVertex(abEdge);
          abVertex.setJoinNode(JoinOrderUtil.createJoinNode(context.plan, abEdge));
          JoinEdge abcEdge = new JoinEdge(thisEdge.getJoinType(), abVertex, cVertex);
          abcEdge = updateJoinQualIfNecessary(context, abcEdge);
          created.add(abcEdge);
        }
      }

      if (!vertex.equals(context.mostLeftVertex)) {
        // case 4: (a - (b - c))
        Set<JoinEdge> bcEdges = TUtil.newHashSet();

        // create edges for (b - c)
        for (JoinEdge abEdge : context.joinEdgesFromChild) {
          if (!abEdge.getLeftVertex().equals(cVertex) && !abEdge.getRightVertex().equals(cVertex)) {
            JoinGroupVertex abVertex = new JoinGroupVertex(abEdge);
            abVertex.setJoinNode(JoinOrderUtil.createJoinNode(context.plan, abEdge));
            JoinVertex bVertex = JoinOrderUtil.findMostRightRelationVertex(abVertex);
            JoinEdge bcEdge = new JoinEdge(thisEdge.getJoinType(), bVertex, cVertex);
            bcEdge = updateJoinQualIfNecessary(context, bcEdge);
            bcEdges.add(bcEdge);
            if (PlannerUtil.isCommutativeJoin(bcEdge.getJoinType())) {
              bcEdges.add(getCommutativeJoinEdge(bcEdge));
            }
          }
        }
        created.addAll(bcEdges);

        // create edges for (a - (b - c))
        JoinEdge abEdge = ((JoinGroupVertex)thisEdge.getLeftVertex()).getJoinEdge();
        Set<JoinVertex> aVertexes = TUtil.newHashSet();
        aVertexes.add(abEdge.getLeftVertex());
        if (PlannerUtil.isCommutativeJoin(abEdge.getJoinType())) {
          aVertexes.add(abEdge.getRightVertex());
        }
        for (JoinEdge bcEdge : bcEdges) {
          for (JoinVertex aVertex : aVertexes) {
            if (!bcEdge.getLeftVertex().equals(aVertex) && !bcEdge.getRightVertex().equals(aVertex)) {
              JoinGroupVertex bcVertex = new JoinGroupVertex(bcEdge);
              bcVertex.setJoinNode(JoinOrderUtil.createJoinNode(context.plan, bcEdge));
              JoinEdge abcEdge = new JoinEdge(abEdge.getJoinType(), aVertex, bcVertex);
              abcEdge = updateJoinQualIfNecessary(context, abcEdge);
              created.add(abcEdge);
            }
          }
        }
      }

      context.joinEdgesFromChild.clear();
      context.joinEdgesFromChild.addAll(created);
      context.totalJoinEdges.addAll(created);

      // handle commutative edges
      for (JoinEdge eachEdge : created) {
        if (PlannerUtil.isCommutativeJoin(eachEdge.getJoinType())) {
          JoinEdge commutativeEdge = getCommutativeJoinEdge(eachEdge);
          context.joinEdgesFromChild.add(commutativeEdge);
          context.totalJoinEdges.add(commutativeEdge);
        }
      }
    }

    private static Set<EvalNode> findConditionsForJoin(Set<EvalNode> candidates, JoinEdge edge) {
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

  public Set<JoinEdge> populateJoinEdges(LogicalPlan plan) {
    // collect join predicates within the group
    Set<EvalNode> predicates = collectJoinPredicates();

    JoinEdgeCollectorContext context = new JoinEdgeCollectorContext(plan, mostLeftVertex,
        mostRightVertex, predicates);
    JoinEdgeCollector collector = new JoinEdgeCollector();
    collector.visit(context, new Stack<JoinVertex>(), this.mostRightVertex);
    return context.totalJoinEdges;
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