package org.apache.tajo.plan.joinorder;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;

public class NonAssociativeGroupVertex implements JoinVertex {
  private final JoinEdge joinEdge;
  private final Schema schema;

  public NonAssociativeGroupVertex(JoinEdge joinEdge) {
    this.joinEdge = joinEdge;
    this.schema = SchemaUtil.merge(joinEdge.getLeftRelation().getSchema(),
        joinEdge.getRightRelation().getSchema());
  }

  public JoinEdge getJoinEdge() {
    return this.joinEdge;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return joinEdge.toString();
  }
}
