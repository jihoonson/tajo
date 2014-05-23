/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.global;

import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.engine.planner.LogicalPlanTree;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.logical.*;

import java.util.*;

/**
 * A distributed execution plan (DEP) is a direct acyclic graph (DAG) of ExecutionBlocks.
 * An ExecutionBlock is a basic execution unit that could be distributed across a number of nodes.
 * An ExecutionBlock class contains input information (e.g., child execution blocks or input
 * tables), and output information (e.g., partition type, partition key, and partition number).
 * In addition, it includes a logical plan to be executed in each node.
 */
public class ExecutionBlock {
  private ExecutionBlockId executionBlockId;
  private LogicalPlanTree plan = null;
  private LogicalNode root = null;
  private StoreTableNode store = null;
  private List<ScanNode> scanlist = new ArrayList<ScanNode>();
  private Enforcer enforcer = new Enforcer();

  private boolean hasJoinPlan;
  private boolean hasUnionPlan;

  private Set<String> broadcasted = new HashSet<String>();

  public ExecutionBlock(ExecutionBlockId executionBlockId) {
    this.executionBlockId = executionBlockId;
  }

  public ExecutionBlockId getId() {
    return executionBlockId;
  }

  public void setPlan(LogicalPlanTree plan, LogicalNode root) {
    hasJoinPlan = false;
    hasUnionPlan = false;
    this.scanlist.clear();
    this.plan = plan;
    this.root = root;

    if (plan == null) {
      return;
    }

    LogicalNode node = this.root;
    ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
    s.add(node);
    while (!s.isEmpty()) {
      node = s.remove(s.size()-1);
      if (node.getType() == NodeType.SCAN) {
        scanlist.add((ScanNode) node);
      } else if (node.getType() == NodeType.JOIN) {
        hasJoinPlan = true;
      } else if (node.getType() == NodeType.UNION) {
        hasUnionPlan = true;
      } else if (node.getType() == NodeType.TABLE_SUBQUERY) {
        s.add(((TableSubQueryNode) node).getSubQuery());
      } else {
        for (LogicalNode child : plan.getChilds(node)) {
          s.add(s.size(), child);
        }
      }
    }
  }


  public LogicalPlanTree getPlan() {
    return plan;
  }

  public Enforcer getEnforcer() {
    return enforcer;
  }

  public StoreTableNode getStoreTableNode() {
    return store;
  }

  public ScanNode [] getScanNodes() {
    return this.scanlist.toArray(new ScanNode[scanlist.size()]);
  }

  public boolean hasJoin() {
    return hasJoinPlan;
  }

  public boolean hasUnion() {
    return hasUnionPlan;
  }

  public void addBroadcastTable(String tableName) {
    broadcasted.add(tableName);
    enforcer.addBroadcast(tableName);
  }

  public boolean isBroadcastTable(String tableName) {
    return broadcasted.contains(tableName);
  }

  public Collection<String> getBroadcastTables() {
    return broadcasted;
  }

  public String toString() {
    return executionBlockId.toString();
  }

  public LogicalNode getRoot() {
    return root;
  }
}
