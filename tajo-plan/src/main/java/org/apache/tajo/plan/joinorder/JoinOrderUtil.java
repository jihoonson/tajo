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

import org.apache.tajo.util.TUtil;

import java.util.Set;
import java.util.Stack;

public class JoinOrderUtil {

  public static Set<RelationVertex> findRelationVertexes(JoinVertex root) {
    RelationVertexFinderContext context = new RelationVertexFinderContext();
    RelationVertexFinder finder = new RelationVertexFinder();
    finder.visit(context, new Stack<JoinVertex>(), root);
    return context.founds;
  }

  public static RelationVertex findMostRightRelationVertex(JoinVertex root) {
    RelationVertexFinderContext context = new RelationVertexFinderContext();
    context.findMostRight = true;
    RelationVertexFinder finder = new RelationVertexFinder();
    finder.visit(context, new Stack<JoinVertex>(), root);
    if (context.founds.isEmpty()) {
      return null;
    } else {
      return context.founds.iterator().next();
    }
  }

  public static class RelationVertexFinderContext {
    boolean findMostLeft = false;
    boolean findMostRight = false;
    Set<RelationVertex> founds = TUtil.newHashSet();
  }

  public static class RelationVertexFinder extends JoinTreeVisitor<RelationVertexFinderContext> {

    @Override
    public void visitRelationVertex(RelationVertexFinderContext context, Stack<JoinVertex> stack, RelationVertex vertex) {
      super.visitRelationVertex(context, stack, vertex);
      context.founds.add(vertex);
    }

    @Override
    public void visitJoinGroupVertex(RelationVertexFinderContext context, Stack<JoinVertex> stack, JoinGroupVertex vertex) {
      if (!context.findMostRight) {
        visit(context, stack, vertex.getJoinEdge().getLeftVertex());
      }
      if (!context.findMostLeft) {
        visit(context, stack, vertex.getJoinEdge().getRightVertex());
      }
    }
  }
}
