/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.joinorder;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;

public class JoinGroupVertex implements JoinVertex {
  private final JoinEdge joinEdge;
  private final Schema schema;
  private JoinNode joinNode; // corresponding join node

  public JoinGroupVertex(JoinEdge joinEdge) {
    this.joinEdge = joinEdge;
    this.schema = SchemaUtil.merge(joinEdge.getLeftVertex().getSchema(),
        joinEdge.getRightVertex().getSchema());
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

  public LogicalNode getCorrespondingNode() {
    return joinNode;
  }

  public void setJoinNode(JoinNode joinNode) {
    this.joinNode = joinNode;
  }

  public JoinType getJoinType() {
    return joinEdge.getJoinType();
  }
}
