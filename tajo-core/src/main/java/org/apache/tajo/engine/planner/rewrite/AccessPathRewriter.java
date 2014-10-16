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

package org.apache.tajo.engine.planner.rewrite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.IndexDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.AccessPathInfo.ScanTypeControl;
import org.apache.tajo.engine.planner.logical.IndexScanNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.RelationNode;
import org.apache.tajo.engine.planner.logical.ScanNode;

import java.util.List;
import java.util.Stack;

public class AccessPathRewriter implements RewriteRule {
  private static final Log LOG = LogFactory.getLog(AccessPathRewriter.class);

  private static final String NAME = "Access Path Rewriter";
  private final Rewriter rewriter = new Rewriter();
  private final float selectivityThreshold;

  public AccessPathRewriter(TajoConf conf) {
    this.selectivityThreshold = conf.getFloatVar(conf, TajoConf.ConfVars.INDEX_SELECTIVITY_THRESHOLD);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      for (RelationNode relationNode : block.getRelations()) {
        List<AccessPathInfo> accessPathInfos = block.getAccessInfos(relationNode);
        // If there are any alternative access paths
        if (accessPathInfos.size() > 1) {
          for (AccessPathInfo accessPathInfo : accessPathInfos) {
            if (accessPathInfo.getScanType() == ScanTypeControl.INDEX_SCAN) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  private double estimateSelectivity(IndexDesc indexDesc, TableStats tableStats) {
    // assume that the key column has a uniform distribution
    for (ColumnStats columnStats : tableStats.getColumnStats()) {
      if (indexDesc.getColumn().equals(columnStats.getColumn())) {
        long distValNum = columnStats.getNumDistValues();
        if (distValNum > 0) {
          long totalRows = tableStats.getNumRows();
          long rowNumPerVal = totalRows / distValNum;
          return (double)rowNumPerVal / (double)totalRows;
        }
        break;
      }
    }
    return -1.d;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();
    rewriter.visit(rootBlock, plan, rootBlock, rootBlock.getRoot(), new Stack<LogicalNode>());
    return plan;
  }

  private final class Rewriter extends BasicLogicalPlanVisitor<Object, Object> {

    @Override
    public Object visitScan(Object object, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode scanNode,
                            Stack<LogicalNode> stack) throws PlanningException {
      List<AccessPathInfo> accessPaths = block.getAccessInfos(scanNode);
      AccessPathInfo optimalPath = null;
      // initialize
      for (AccessPathInfo accessPath : accessPaths) {
        if (accessPath.getScanType() == ScanTypeControl.SEQ_SCAN) {
          optimalPath = accessPath;
        }
      }
      // find the optimal path
      for (AccessPathInfo accessPath : accessPaths) {
        if (accessPath.getScanType() == ScanTypeControl.INDEX_SCAN) {
          // estimation selectivity and choose the better path
          IndexScanInfo indexScanInfo = (IndexScanInfo) accessPath;
          if (indexScanInfo.hasTableStats()) {
            // TODO: improve the selectivity estimation
            // estimate the selectivity
            double estimateSelectivity = estimateSelectivity(indexScanInfo.getIndexDesc(),
                indexScanInfo.getTableStats());
            LOG.info("Estimated selectivity: " + estimateSelectivity);
            if (estimateSelectivity > selectivityThreshold) {
              // if the estimated selectivity is greater than threshold, use the index scan
              optimalPath = accessPath;
            }
          }
        }
      }

      if (optimalPath != null && optimalPath.getScanType() == ScanTypeControl.INDEX_SCAN) {
        IndexScanInfo indexScanInfo = (IndexScanInfo) optimalPath;
        plan.addHistory("AccessPathRewriter chooses " + indexScanInfo.getIndexDesc().getName() + " for "
            + scanNode.getTableName() + " scan");
        IndexDesc indexDesc = indexScanInfo.getIndexDesc();
        Schema indexKeySchema = new Schema(new Column[]{indexDesc.getColumn()});
        SortSpec[] sortSpecs = new SortSpec[1];
        sortSpecs[0] = new SortSpec(indexDesc.getColumn(), indexDesc.isAscending(), false);
        IndexScanNode indexScanNode = new IndexScanNode(plan.newPID(), scanNode, indexKeySchema,
            indexScanInfo.getValues(), sortSpecs, indexDesc.getIndexPath());
        if (stack.empty() || block.getRoot().equals(scanNode)) {
          block.setRoot(indexScanNode);
        } else {
          PlannerUtil.replaceNode(plan, stack.peek(), scanNode, indexScanNode);
        }
      }
      return null;
    }
  }
}
