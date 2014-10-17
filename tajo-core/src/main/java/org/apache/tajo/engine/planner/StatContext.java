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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.proto.CatalogProtos.StatType;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Map;

public class StatContext {

  private static final Log LOG = LogFactory.getLog(StatContext.class);
  private Map<Column, List<StatType>> enabledColumnStats = TUtil.newHashMap();

  public StatContext() {

  }

  public StatContext(List<TajoWorkerProtocol.EnforceProperty> properties) {
    this.setStatEnabledColumns(properties);
  }

  public void setStatEnabledColumns(List<TajoWorkerProtocol.EnforceProperty> properties) {
    for (TajoWorkerProtocol.EnforceProperty property : properties) {
      TajoWorkerProtocol.ColumnStatEnforcer enforcer = property.getColumnStat();
      enabledColumnStats.put(new Column(enforcer.getColumn()), enforcer.getCollectTypesList());
      if (enforcer.getCollectTypesList().contains(StatType.COLUMN_HISTOGRAM) ||
          enforcer.getCollectTypesList().contains(StatType.COLUMN_NUM_DIST_VALS)) {
        LOG.warn("Stat types " + StatType.COLUMN_HISTOGRAM + " and " + StatType.COLUMN_NUM_DIST_VALS +
            " are not supported yet");
      }
    }
  }

  public boolean hasStatEnabledColumns() {
    return enabledColumnStats.size() > 0;
  }

  public boolean isStatEnabled(Column column) {
    return enabledColumnStats.containsKey(column);
  }

  public List<StatType> getEnabledStatTypes(Column column) {
    return enabledColumnStats.get(column);
  }
}
