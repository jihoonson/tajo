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

import com.google.common.collect.Sets;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.RelationNode;
import org.apache.tajo.util.TUtil;

import java.util.Collections;
import java.util.Set;

public class JoinEdge {
//  private final JoinType joinType;
//  private final LogicalNode leftRelation;
//  private final LogicalNode rightRelation;
  private final JoinType joinType;
  private final Set<EvalNode> joinQual = Sets.newHashSet();
  private final RelationNode leftRelation;
  private final RelationNode rightRelation;

  public JoinEdge(JoinType joinType, RelationNode leftRelation, RelationNode rightRelation) {
    this(joinType, leftRelation, rightRelation, null);
  }

  public JoinEdge(JoinType joinType, RelationNode leftRelation, RelationNode rightRelation,
                  @Nullable Set<EvalNode> joinQual) {
    this.joinType = joinType;
//    this.leftRelation = leftRelation;
//    this.rightRelation = rightRelation;
    this.leftRelation = leftRelation;
    this.rightRelation = rightRelation;
    if (joinQual != null) {
      this.joinQual.addAll(joinQual);
    }
  }

//  public JoinEdge(JoinType joinType, LogicalNode leftRelation, LogicalNode rightRelation,
//                  EvalNode ... condition) {
//    this(joinType, leftRelation, rightRelation);
//    Collections.addAll(joinQual, condition);
//  }

  public JoinType getJoinType() {
    return joinType;
  }

  public LogicalNode getLeftRelation() {
    return leftRelation;
  }

  public LogicalNode getRightRelation() {
    return rightRelation;
  }

  public boolean hasJoinQual() {
    return !joinQual.isEmpty();
  }

  public void addJoinQual(EvalNode joinQual) {
    this.joinQual.add(joinQual);
  }

  public EvalNode [] getJoinQual() {
    return joinQual.toArray(new EvalNode[joinQual.size()]);
  }

  public String toString() {
    return getLeftRelation() + " " + getJoinType() + " " + getRightRelation() + " ON "
        + TUtil.collectionToString(joinQual, ", ");
  }
}
