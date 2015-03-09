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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.util.TUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class AssociativeGroupVertex implements JoinVertex {
  private Set<JoinVertex> vertexes = TUtil.newHashSet();
  private Map<VertexPair, JoinEdge> joinEdges = TUtil.newHashMap();
//  private Set<EvalNode> predicates = TUtil.newHashSet();

  public void addVertex(JoinVertex vertex) {
    this.vertexes.add(vertex);
  }

//  public void addPredicate(EvalNode predicate) {
//    this.predicates.add(predicate);
//  }

//  public void addPredicates(Set<EvalNode> predicates) {
//    this.predicates.addAll(predicates);
//  }

  public void addJoinEdge(JoinVertex left, JoinVertex right, JoinEdge edge) {
    this.joinEdges.put(new VertexPair(left, right), edge);
  }

  public void addJoinEdge(JoinEdge joinEdge) {
//    addVertex(joinEdge.getLeftRelation());
//    addVertex(joinEdge.getRightRelation());
    this.addJoinEdge(joinEdge.getLeftRelation(), joinEdge.getRightRelation(), joinEdge);
  }

  public boolean isEmpty() {
    return vertexes.isEmpty();
  }

  public Set<JoinVertex> getVertexes() {
    return vertexes;
  }

  public Map<VertexPair, JoinEdge> getJoinEdgeMap() {
    return joinEdges;
  }

  public Collection<JoinEdge> getJoinEdges() {
    return joinEdges.values();
  }

  @Override
  public String toString() {
    return "V (" + TUtil.collectionToString(vertexes, ",") + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof AssociativeGroupVertex) {
      AssociativeGroupVertex other = (AssociativeGroupVertex) o;
      return TUtil.checkEquals(this.vertexes, other.vertexes);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return vertexes.hashCode();
  }

  @Override
  public Schema getSchema() {
    Schema schema = new Schema();
    for (JoinVertex v : vertexes) {
      schema = SchemaUtil.merge(schema, v.getSchema());
    }
    return schema;
  }
}