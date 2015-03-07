package org.apache.tajo.plan.joinorder;

import org.apache.tajo.plan.logical.RelationNode;

public class RelationVertex extends JoinVertex {
  private RelationNode relationNode;

  public RelationVertex(RelationNode relationNode) {
    this.relationNode = relationNode;
  }

  @Override
  public String toString() {
    return relationNode.getCanonicalName();
  }
}
