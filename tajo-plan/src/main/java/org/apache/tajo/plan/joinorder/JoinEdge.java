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

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.util.TUtil;

import java.util.Collections;
import java.util.Set;

public class JoinEdge {
  private JoinType joinType;
  private final JoinVertex leftVertex;
  private final JoinVertex rightVertex;
  private final Set<EvalNode> joinPredicates = Sets.newHashSet();

  public JoinEdge(JoinType joinType, JoinVertex leftVertex, JoinVertex rightVertex) {
    this.joinType = joinType;
    this.leftVertex = leftVertex;
    this.rightVertex = rightVertex;
  }

//  public JoinEdge(JoinType joinType, LogicalNode leftRelation, LogicalNode rightRelation,
//                  EvalNode ... condition) {
//    this(joinType, leftRelation, rightRelation);
//    Collections.addAll(joinQual, condition);
//  }

  public void setJoinType(JoinType joinType) {
    this.joinType = joinType;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public JoinVertex getLeftVertex() {
    return leftVertex;
  }

  public JoinVertex getRightVertex() {
    return rightVertex;
  }

  public boolean hasJoinQual() {
    return joinPredicates.size() > 0;
  }

  public void addJoinQual(EvalNode joinQual) {
    this.joinPredicates.add(joinQual);
  }

  public void addJoinQuals(Set<EvalNode> joinQuals) {
    this.joinPredicates.addAll(joinQuals);
  }

  public void setJoinQuals(Set<EvalNode> joinPredicates) {
    this.joinPredicates.clear();
    this.joinPredicates.addAll(joinPredicates);
  }

  public EvalNode [] getJoinQual() {
    return joinPredicates.toArray(new EvalNode[joinPredicates.size()]);
  }

  public String toString() {
    return leftVertex + " " + joinType + " " + rightVertex + " ON " + TUtil.collectionToString(joinPredicates, ", ");
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(joinType, leftVertex, rightVertex, joinPredicates.hashCode());
  }

  public boolean equals(Object o) {
    if (o instanceof JoinEdge) {
      JoinEdge other = (JoinEdge) o;
      boolean eq = this.joinType == other.joinType;
      eq &= this.leftVertex.equals(other.leftVertex);
      eq &= this.rightVertex.equals(other.rightVertex);
      eq &= this.joinPredicates.equals(other.joinPredicates);
      return eq;
    }
    return false;
  }
}
