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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.util.TUtil;

import java.util.List;

public class StatContext {

  private Column[] statEnabledColumns;

  public StatContext() {

  }

  public StatContext(List<TajoWorkerProtocol.EnforceProperty> properties) {
    this.setStatEnabledColumns(properties);
  }

  public void setStatEnabledColumns(List<TajoWorkerProtocol.EnforceProperty> properties) {
    List<Column> columns = TUtil.newList();
    for (TajoWorkerProtocol.EnforceProperty property : properties) {
      TajoWorkerProtocol.ColumnStatEnforcer columnStatEnforcer = property.getColumnStat();
      if (!columnStatEnforcer.hasCollect()
          || columnStatEnforcer.getCollect()) {
        columns.add(new Column(columnStatEnforcer.getColumn()));
      }
    }
    statEnabledColumns = columns.toArray(new Column[columns.size()]);
  }

  public Column[] getStatEnabledColumns() {
    return this.statEnabledColumns;
  }

  public boolean hasStatEnabledColumns() {
    return statEnabledColumns != null;
  }
}
