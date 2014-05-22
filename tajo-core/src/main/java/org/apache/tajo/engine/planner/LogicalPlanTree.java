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
import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.LogicalPlan.PidFactory;
import org.apache.tajo.engine.planner.graph.SimpleTree;
import org.apache.tajo.engine.planner.logical.ArityClass;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.LogicalNode.EdgeType;
import org.apache.tajo.engine.planner.logical.LogicalNode.LogicalNodeEdge;
import org.apache.tajo.engine.planner.logical.LogicalNodeVisitor;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalPlanTree extends SimpleTree<Integer, LogicalNodeEdge>
    implements GsonObject, Cloneable {
  @Expose private Map<Integer, LogicalNode> nodeMap = TUtil.newHashMap();
  @Expose private PidFactory pidFactory;

  public LogicalPlanTree(PidFactory pidFactory) {
    this.pidFactory = pidFactory;
  }

  public void addChild(LogicalNode child, LogicalNode parent) {
    nodeMap.put(child.getPID(), child);
    nodeMap.put(parent.getPID(), parent);
    this.addEdge(child.getPID(), parent.getPID(),
        new LogicalNodeEdge(parent.getPID(), child.getPID(), EdgeType.UNORDERED));
  }

  public void setChild(LogicalNode child, LogicalNode parent) {
    Integer pid = getChild(parent.getPID(), 0);
    if (pid != null) {
      removeEdge(pid, parent.getPID());
      nodeMap.remove(pid);
    }
    pid = getParent(child.getPID());
    if (pid != null) {
      removeEdge(child.getPID(), pid);
      nodeMap.remove(pid);
    }
    addChild(child, parent);
  }

  public void setLeftChild(LogicalNode child, LogicalNode parent) {
    Integer pid = getLeftChildPid(parent.getPID());
    if (pid != null) {
      removeEdge(pid, parent.getPID());
      nodeMap.remove(pid);
    }
    pid = getParent(child.getPID());
    if (pid != null) {
      removeEdge(child.getPID(), pid);
      nodeMap.remove(pid);
    }

    nodeMap.put(child.getPID(), child);
    nodeMap.put(parent.getPID(), parent);
    this.addEdge(child.getPID(), parent.getPID(),
        new LogicalNodeEdge(parent.getPID(), child.getPID(), EdgeType.ORDERED_LEFT));
  }

  public void setRightChild(LogicalNode child, LogicalNode parent) {
    Integer pid = getRightChildPid(parent.getPID());
    if (pid != null) {
      removeEdge(pid, parent.getPID());
      nodeMap.remove(pid);
    }
    pid = getParent(child.getPID());
    if (pid != null) {
      removeEdge(child.getPID(), pid);
      nodeMap.remove(pid);
    }

    nodeMap.put(child.getPID(), child);
    nodeMap.put(parent.getPID(), parent);
    this.addEdge(child.getPID(), parent.getPID(),
        new LogicalNodeEdge(parent.getPID(), child.getPID(), EdgeType.ORDERED_RIGHT));
  }

  public <NODE extends LogicalNode> NODE getParent(LogicalNode child) {
    Integer parentPid = this.getParent(child.getPID());
    if (parentPid != null) {
      return (NODE) nodeMap.get(parentPid);
    }
    return null;
  }

  public boolean hasChild(LogicalNode parent) {
    return getChildCount(parent.getPID()) > 0;
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
      Preconditions.checkState(edges.size() <= 2);
      for (int i = 0; i < edges.size(); i++) {
        if (edges.get(i).getEdgeType() == EdgeType.ORDERED_LEFT) {
          return edges.get(i).getChildPid();
        }
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
      Preconditions.checkState(edges.size() <= 2);
      for (int i = 0; i < edges.size(); i++) {
        if (edges.get(i).getEdgeType() == EdgeType.ORDERED_RIGHT) {
          return edges.get(i).getChildPid();
        }
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

  public <NODE extends LogicalNode> List<NODE> getChildsInOrder(LogicalNode parent) {
    List<NODE> childNodes = TUtil.newList();
    childNodes.add(this.<NODE>getLeftChild(parent));
    childNodes.add(this.<NODE>getRightChild(parent));
    return childNodes;
  }

  private boolean isAvailableEdgeType(LogicalNode parent, EdgeType edgeType) {
    switch (parent.getType()) {
      case ROOT:
      case PROJECTION:
      case LIMIT:
      case SORT:
      case HAVING:
      case GROUP_BY:
      case SELECTION:
        return edgeType == EdgeType.UNORDERED;
      case EXPRS:
      case SCAN:
      case PARTITIONS_SCAN:
      case BST_INDEX_SCAN:
        return false;
      case JOIN:
      case UNION:
      case EXCEPT:
      case INTERSECT:
        return edgeType == EdgeType.ORDERED_LEFT || edgeType == EdgeType.ORDERED_RIGHT;
      case TABLE_SUBQUERY:
        break;
      case STORE:
        break;
      case INSERT:
        break;
      case CREATE_DATABASE:
        break;
      case DROP_DATABASE:
        break;
      case CREATE_TABLE:
        break;
      case DROP_TABLE:
        break;
      case ALTER_TABLESPACE:
        break;
      case ALTER_TABLE:
        break;
    }
    return false;
  }

  public void preOrder(LogicalNodeVisitor visitor, LogicalNode current) {
    visitor.visit(current);
    if (ArityClass.getArityClassByNodeType(current.getType()) == ArityClass.UNARY) {
      preOrder(visitor, getChild(current));
    } else if (ArityClass.getArityClassByNodeType(current.getType()) == ArityClass.BINARY) {
      for (LogicalNode child : getChildsInOrder(current)) {
        preOrder(visitor, child);
      }
    }
  }

  public void postOrder(LogicalNodeVisitor visitor, LogicalNode current) {
    if (ArityClass.getArityClassByNodeType(current.getType()) == ArityClass.UNARY) {
      postOrder(visitor, getChild(current));
    } else if (ArityClass.getArityClassByNodeType(current.getType()) == ArityClass.BINARY) {
      for (LogicalNode child : getChildsInOrder(current)) {
        postOrder(visitor, child);
      }
    }
	  visitor.visit(current);
  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, LogicalPlanTree.class);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    LogicalPlanTree clone = new LogicalPlanTree(pidFactory);
    Map<Integer, Integer> cloneNodeMap = new HashMap<Integer, Integer>();

    for (LogicalNode eachNode : nodeMap.values()) {
      LogicalNode cloneNode = (LogicalNode) eachNode.clone();
      cloneNode.setPID(pidFactory.newPid());
      clone.nodeMap.put(cloneNode.getPID(), cloneNode);
      cloneNodeMap.put(eachNode.getPID(), cloneNode.getPID());
    }

    for (LogicalNodeEdge eachEdge : getEdgesAll()) {
      Integer cloneChild = cloneNodeMap.get(eachEdge.getChildPid());
      Integer cloneParent = cloneNodeMap.get(eachEdge.getParentPid());
      clone.addEdge(cloneChild, cloneParent,
          new LogicalNodeEdge(cloneChild, cloneParent, eachEdge.getEdgeType()));
    }
    return clone;
  }

  public LogicalNode getRootNode() {
    if (root != null) {
      return nodeMap.get(root);
    } else {
      return null;
    }
  }
}
