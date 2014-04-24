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

  public void addChild(LogicalNode child, LogicalNode parent) {
    nodeMap.put(child.getPID(), child);
    nodeMap.put(parent.getPID(), parent);
    this.addEdge(child.getPID(), parent.getPID(),
        new LogicalNodeEdge(child.getPID(), parent.getPID(), EdgeType.UNORDERED));
  }

  public void setChild(LogicalNode child, LogicalNode parent) {
    Integer childPid = getChild(parent.getPID(), 0);
    if (childPid != null) {
      removeEdge(childPid, parent.getPID());
      nodeMap.remove(childPid);
    }
    addChild(child, parent);
  }

  public void setLeftChild(LogicalNode child, LogicalNode parent) {
    Integer leftChildPid = getLeftChildPid(parent.getPID());
    if (leftChildPid != null) {
      removeEdge(leftChildPid, parent.getPID());
      nodeMap.remove(leftChildPid);
    }

    nodeMap.put(child.getPID(), child);
    nodeMap.put(parent.getPID(), parent);
    this.addEdge(child.getPID(), parent.getPID(),
        new LogicalNodeEdge(child.getPID(), parent.getPID(), EdgeType.ORDERED_LEFT));
  }

  public void setRightChild(LogicalNode child, LogicalNode parent) {
    Integer rightChildPid = getRightChildPid(parent.getPID());
    if (rightChildPid != null) {
      removeEdge(rightChildPid, parent.getPID());
      nodeMap.remove(rightChildPid);
    }

    nodeMap.put(child.getPID(), child);
    nodeMap.put(parent.getPID(), parent);
    this.addEdge(child.getPID(), parent.getPID(),
        new LogicalNodeEdge(child.getPID(), parent.getPID(), EdgeType.ORDERED_RIGHT));
  }

  public <NODE extends LogicalNode> NODE getParent(LogicalNode child) {
    Integer parentPid = this.getParent(child.getPID());
    if (parentPid != null) {
      return (NODE) nodeMap.get(parentPid);
    }
    return null;
  }

  public <NODE extends LogicalNode> NODE getChild(LogicalNode parent) {
    List<LogicalNodeEdge> edges = this.getIncomingEdges(parent.getPID());
    if (edges != null) {
      Preconditions.checkState(edges.size() == 1);
      Preconditions.checkState(edges.get(0).getEdgeType() == EdgeType.UNORDERED);
      return (NODE) nodeMap.get(edges.get(0).getChildPid());
    }
    return null;
  }

  public <NODE extends LogicalNode> NODE getLeftChild(LogicalNode parent) {
    Integer leftChildPid = getLeftChildPid(parent.getPID());
    if (leftChildPid != null) {
      return (NODE) nodeMap.get(leftChildPid);
    }
    return null;
  }

  private Integer getLeftChildPid(Integer parentPid) {
    List<LogicalNodeEdge> edges = this.getIncomingEdges(parentPid);
    if (edges != null) {
      Preconditions.checkState(edges.size() == 2);
      if (edges.get(0).getEdgeType() == EdgeType.ORDERED_LEFT) {
        return edges.get(0).getChildPid();
      } else if (edges.get(1).getEdgeType() == EdgeType.ORDERED_LEFT) {
        return edges.get(1).getChildPid();
      }
    }
    return null;
  }

  public <NODE extends LogicalNode> NODE getRightChild(LogicalNode parent) {
    Integer rightChildPid = getRightChildPid(parent.getPID());
    if (rightChildPid != null) {
      return (NODE) nodeMap.get(rightChildPid);
    }
    return null;
  }

  private Integer getRightChildPid(Integer parentPid) {
    List<LogicalNodeEdge> edges = this.getIncomingEdges(parentPid);
    if (edges != null) {
      Preconditions.checkState(edges.size() == 2);
      if (edges.get(0).getEdgeType() == EdgeType.ORDERED_RIGHT) {
        return edges.get(0).getChildPid();
      } else if (edges.get(1).getEdgeType() == EdgeType.ORDERED_RIGHT) {
        return edges.get(1).getChildPid();
      }
    }
    return null;
  }

  public <NODE extends LogicalNode> NODE getChild(LogicalNode parent, int i) {
    Integer childPid = this.getChild(parent.getPID(), i);
    if (childPid != null) {
      return (NODE) nodeMap.get(childPid);
    }
    return null;
  }

  public <NODE extends LogicalNode> List<NODE> getChilds(LogicalNode parent) {
    List<Integer> childPids = this.getChilds(parent.getPID());
    List<NODE> childNodes = TUtil.newList();
    for (Integer childPid : childPids) {
      childNodes.add((NODE) nodeMap.get(childPid));
    }
    return childNodes;
  }
}
