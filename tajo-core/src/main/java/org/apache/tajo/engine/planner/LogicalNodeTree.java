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

package org.apache.tajo.engine.planner;

import com.google.common.base.Preconditions;
import org.apache.tajo.engine.planner.graph.SimpleTree;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalNode.EdgeType;
import org.apache.tajo.engine.planner.logical.LogicalNode.LogicalNodeEdge;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Map;

public class LogicalNodeTree extends SimpleTree<Integer, LogicalNodeEdge> {
  private Map<Integer, LogicalNode> nodeMap = TUtil.newHashMap();

  public void add(LogicalNode child, LogicalNode parent) {
    nodeMap.put(child.getPID(), child);
    nodeMap.put(parent.getPID(), parent);
    this.addEdge(child.getPID(), parent.getPID(),
        new LogicalNodeEdge(child.getPID(), parent.getPID(), EdgeType.UNORDERED));
  }

  public void addLeftChild(LogicalNode child, LogicalNode parent) {
    nodeMap.put(child.getPID(), child);
    nodeMap.put(parent.getPID(), parent);
    this.addEdge(child.getPID(), parent.getPID(),
        new LogicalNodeEdge(child.getPID(), parent.getPID(), EdgeType.ORDERED_LEFT));
  }

  public void addRightChild(LogicalNode child, LogicalNode parent) {
    nodeMap.put(child.getPID(), child);
    nodeMap.put(parent.getPID(), parent);
    this.addEdge(child.getPID(), parent.getPID(),
        new LogicalNodeEdge(child.getPID(), parent.getPID(), EdgeType.ORDERED_RIGHT));
  }

  public LogicalNode getParent(LogicalNode child) {
    Integer parentPid = this.getParent(child.getPID());
    if (parentPid != null) {
      return nodeMap.get(parentPid);
    }
    return null;
  }

  public LogicalNode getChild(LogicalNode parent) {
    List<LogicalNodeEdge> edges = this.getIncomingEdges(parent.getPID());
    if (edges != null) {
      Preconditions.checkState(edges.size() == 1);
      Preconditions.checkState(edges.get(0).getEdgeType() == EdgeType.UNORDERED);
      return nodeMap.get(edges.get(0).getChildPid());
    }
    return null;
  }

  public LogicalNode getLeftChild(LogicalNode parent) {
    List<LogicalNodeEdge> edges = this.getIncomingEdges(parent.getPID());
    if (edges != null) {
      Preconditions.checkState(edges.size() == 2);
      if (edges.get(0).getEdgeType() == EdgeType.ORDERED_LEFT) {
        return nodeMap.get(edges.get(0).getChildPid());
      } else if (edges.get(1).getEdgeType() == EdgeType.ORDERED_LEFT) {
        return nodeMap.get(edges.get(1).getChildPid());
      }
    }
    return null;
  }

  public LogicalNode getRightChild(LogicalNode parent) {
    List<LogicalNodeEdge> edges = this.getIncomingEdges(parent.getPID());
    if (edges != null) {
      Preconditions.checkState(edges.size() == 2);
      if (edges.get(0).getEdgeType() == EdgeType.ORDERED_RIGHT) {
        return nodeMap.get(edges.get(0).getChildPid());
      } else if (edges.get(1).getEdgeType() == EdgeType.ORDERED_RIGHT) {
        return nodeMap.get(edges.get(1).getChildPid());
      }
    }
    return null;
  }

  public LogicalNode getChild(LogicalNode parent, int i) {
    Integer childPid = this.getChild(parent.getPID(), i);
    if (childPid != null) {
      return nodeMap.get(childPid);
    }
    return null;
  }

  public List<LogicalNode> getChilds(LogicalNode parent) {
    List<Integer> childPids = this.getChilds(parent.getPID());
    List<LogicalNode> childNodes = TUtil.newList();
    for (int i = 0; i < childPids.size(); i++) {
      childNodes.add(nodeMap.get(childPids.get(i)));
    }
    return childNodes;
  }
}
