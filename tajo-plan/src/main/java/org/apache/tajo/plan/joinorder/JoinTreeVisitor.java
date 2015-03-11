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

import java.util.Stack;

public class JoinTreeVisitor<CONTEXT> {

  public void visit(CONTEXT context, Stack<JoinVertex> stack, JoinVertex vertex) {
    stack.push(vertex);
    if (vertex instanceof RelationVertex) {
      visitRelationVertex(context, stack, (RelationVertex) vertex);
    } else {
      // JoinGroupVertex
      visitJoinGroupVertex(context, stack, (JoinGroupVertex) vertex);
    }
    stack.pop();
  }

  public void visitRelationVertex(CONTEXT context, Stack<JoinVertex> stack, RelationVertex vertex) {
  }

  public void visitJoinGroupVertex(CONTEXT context, Stack<JoinVertex> stack, JoinGroupVertex vertex) {
    visit(context, stack, vertex.getJoinEdge().getLeftVertex());
    visit(context, stack, vertex.getJoinEdge().getRightVertex());
  }
}
