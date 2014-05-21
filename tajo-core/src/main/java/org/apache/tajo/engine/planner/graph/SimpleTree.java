/*
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

package org.apache.tajo.engine.planner.graph;

import com.google.common.base.Preconditions;
import org.apache.tajo.exception.UnsupportedException;

import java.util.List;

public class SimpleTree<V,E> extends SimpleDirectedGraph<V,E> {

  @Override
  public void addEdge(V tail, V head, E edge) {
    if (directedEdges.containsKey(tail)) {
      Preconditions.checkState(getParentCount(tail) == 0);
    }
    super.addEdge(tail, head, edge);
  }

  public V getParent(V v) {
    return super.getParent(v, 0);
  }

  @Override
  public V getParent(V block, int idx) {
    throw new UnsupportedException("Cannot support getParent(V v, int idx) in SimpleTree");
  }

  @Override
  public List<V> getParents(V block) {
    throw new UnsupportedException("Cannot support getParents(V v) in SimpleTree");
  }
}
