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

package org.apache.tajo.catalog.statistics;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnStatsProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.proto.CatalogProtos.BlockStatsProto;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.List;

public class BlockStats implements ProtoObject<BlockStatsProto>, Cloneable, GsonObject {

  private final BlockStatsProto.Builder builder;
  @Expose private String blockId;
  @Expose private List<ColumnStats> columnStats;

  public BlockStats() {
    builder = BlockStatsProto.newBuilder();
    reset();
  }

  public BlockStats(BlockStatsProto proto) {
    this();
    this.blockId = proto.getBlockId();
    for (ColumnStatsProto columnStatsProto : proto.getColStatsList()) {
      if (columnStatsProto.getColumn().getDataType().getType() != Type.PROTOBUF) {
        columnStats.add(new ColumnStats(columnStatsProto));
      }
    }
  }

  public void reset() {
    blockId = null;
    columnStats = TUtil.newList();
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, BlockStats.class);
  }

  @Override
  public BlockStatsProto getProto() {
    builder.clear();
    builder.setBlockId(blockId);
    for (ColumnStats eachColStat : columnStats) {
      builder.addColStats(eachColStat.getProto());
    }
    return builder.build();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    BlockStats clone = new BlockStats();
    clone.blockId = this.blockId;
    clone.columnStats = new ArrayList<ColumnStats>(this.columnStats);
    return clone;
  }

  @Override
  public String toString() {
    Gson gson = CatalogGsonHelper.getPrettyInstance();
    return gson.toJson(this);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BlockStats) {
      BlockStats other = (BlockStats) o;
      if (this.blockId.equals(other.blockId) &&
          TUtil.checkEquals(this.columnStats, other.columnStats)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(blockId, columnStats);
  }

  public String getBlockId() {
    return blockId;
  }

  public List<ColumnStats> getColumnStats() {
    return this.columnStats;
  }
}
