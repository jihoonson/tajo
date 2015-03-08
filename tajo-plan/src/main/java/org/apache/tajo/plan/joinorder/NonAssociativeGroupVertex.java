package org.apache.tajo.plan.joinorder;

public class NonAssociativeGroupVertex {
  private JoinEdge joinEdge;

  public NonAssociativeGroupVertex(JoinEdge joinEdge) {
    this.joinEdge = joinEdge;
  }

  public JoinEdge getJoinEdge() {
    return this.joinEdge;
  }
}
