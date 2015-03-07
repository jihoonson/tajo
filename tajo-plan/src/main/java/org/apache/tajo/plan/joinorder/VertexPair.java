package org.apache.tajo.plan.joinorder;

import org.apache.tajo.util.Pair;

public class VertexPair extends Pair<JoinVertex, JoinVertex> {
  public VertexPair(JoinVertex first, JoinVertex second) {
    super(first, second);
  }
}
