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

package org.apache.tajo.engine.planner;

import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;

import java.util.Stack;

public class BroadcastPlanAnnotator {

  public void annotate(long broadcastSizeThreshold, LogicalPlan plan, LogicalPlan.QueryBlock block, LogicalNode planRoot)
      throws PlanningException {
    VisitorContext context = new VisitorContext(broadcastSizeThreshold);
    Visitor visitor = new Visitor();
    visitor.visit(context, plan, block, planRoot, new Stack<LogicalNode>());
  }

  private static class VisitorContext {
    private final long broadcastTableSizeThreshold;
    private Stack<Integer> largeRelationNumbers = new Stack<Integer>();

    public VisitorContext(long broadcastTableSizeThreshold) {
      this.broadcastTableSizeThreshold = broadcastTableSizeThreshold;
    }
  }

  private static class Visitor extends BasicLogicalPlanVisitor<VisitorContext, LogicalNode> {

    @Override
    public LogicalNode visitScan(VisitorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
      if (isBroadcastable(context, node)) {
        node.enableBroadcast();
        context.largeRelationNumbers.push(0);
      } else {
        context.largeRelationNumbers.push(1);
      }
      return null;
    }

    @Override
    public LogicalNode visitJoin(VisitorContext context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                                 Stack<LogicalNode> stack) throws PlanningException {
      super.visitJoin(context, plan, block, node, stack);
      if (isBroadcastable(context, node)) {
        node.enableBroadcast();
      } else {
        context.largeRelationNumbers.clear();
        context.largeRelationNumbers.push(1);
      }
      return null;
    }

    private static boolean isBroadcastable(VisitorContext context, ScanNode scanNode) {
      return getTableVolume(scanNode) < context.broadcastTableSizeThreshold;
    }

    private static boolean isBroadcastable(VisitorContext context, JoinNode joinNode) {
      // TODO: consider when the child node is not neither scan nor join

      LogicalNode left = joinNode.getLeftChild();
      LogicalNode right = joinNode.getRightChild();
      boolean leftEnabled = false, rightEnabled = false;

      if (left instanceof Broadcastable) {
        Broadcastable leftBroadcastable = (Broadcastable) left;
        if (leftBroadcastable.isBroadcastEnabled()) {
          leftEnabled = true;
        }
      }
      if (right instanceof Broadcastable) {
        Broadcastable rightBroadcastable = (Broadcastable) right;
        if (rightBroadcastable.isBroadcastEnabled()) {
          rightEnabled = true;
        }
      }
      Integer largeRelationNumFromChild = context.largeRelationNumbers.pop() + context.largeRelationNumbers.pop();
      boolean hasTwoLargeChilds = largeRelationNumFromChild >= 2;
      context.largeRelationNumbers.push(largeRelationNumFromChild);

      return !hasTwoLargeChilds && (leftEnabled || rightEnabled);
    }

    /**
     * Get a volume of a table of a partitioned table
     * @param scanNode ScanNode corresponding to a table
     * @return table volume (bytes)
     */
    private static long getTableVolume(ScanNode scanNode) {
      long scanBytes = scanNode.getTableDesc().getStats().getNumBytes();
      if (scanNode.getType() == NodeType.PARTITIONS_SCAN) {
        PartitionedTableScanNode pScanNode = (PartitionedTableScanNode)scanNode;
        if (pScanNode.getInputPaths() == null || pScanNode.getInputPaths().length == 0) {
          scanBytes = 0L;
        }
      }

      return scanBytes;
    }
  }
}
