package org.apache.tajo.plan.joinorder;

import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.util.TUtil;

import java.util.Set;

public class AssociativeGroup extends JoinVertex {
  private Set<JoinVertex> vertexes = TUtil.newHashSet();
  //  private Map<VertexPair, JoinEdge> joinEdges;
  private Set<EvalNode> predicates = TUtil.newHashSet();

  public void addVertex(JoinVertex vertex) {
    this.vertexes.add(vertex);
  }

  public void addPredicate(EvalNode predicate) {
    this.predicates.add(predicate);
  }

  public void addPredicates(Set<EvalNode> predicates) {
    this.predicates.addAll(predicates);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("V (");
    for (JoinVertex v : vertexes) {
      sb.append(v).append(",");
    }
    sb.deleteCharAt(sb.length() - 1).append(")");
    if (!predicates.isEmpty()) {
      sb.append(", predicates (");
      for (EvalNode p : predicates) {
        sb.append(p).append(",");
      }
      sb.deleteCharAt(sb.length() - 1).append(")");
    }
    return sb.toString();
  }
}