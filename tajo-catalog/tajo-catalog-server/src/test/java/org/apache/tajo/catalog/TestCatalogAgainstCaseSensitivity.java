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

package org.apache.tajo.catalog;

import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceCommand;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.AlterTablespaceType;
import org.apache.tajo.catalog.proto.CatalogProtos.AlterTablespaceProto.SetLocation;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescriptorProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TablespaceProto;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class TestCatalogAgainstCaseSensitivity {
  static CatalogServer server;
  static CatalogService catalog;

  @BeforeClass
  public static void setup() throws Exception {
    server = new MiniCatalogServer();
    catalog = new LocalCatalogWrapper(server);
    CatalogTestingUtil.prepareBaseData(catalog, ((MiniCatalogServer)server).getTestDir());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    CatalogTestingUtil.cleanupBaseData(catalog);
    server.stop();
  }

  @Test
  public void testTablespace() throws Exception {
    assertTrue(catalog.existTablespace("space1"));
    assertTrue(catalog.existTablespace("SpAcE1"));

    catalog.alterTablespace(AlterTablespaceProto.newBuilder().
        setSpaceName("space1").
        addCommand(
            AlterTablespaceCommand.newBuilder().
                setType(AlterTablespaceType.LOCATION).
                setLocation(SetLocation.newBuilder()
                    .setUri("hdfs://zzz.com/warehouse"))).build());

    catalog.alterTablespace(AlterTablespaceProto.newBuilder().
        setSpaceName("SpAcE1").
        addCommand(
            AlterTablespaceCommand.newBuilder().
                setType(AlterTablespaceType.LOCATION).
                setLocation(SetLocation.newBuilder()
                    .setUri("hdfs://zzz.com/warehouse"))).build());

    Set<TablespaceProto> tablespaceProtos = new HashSet<>();
    for (String tablespaceName : catalog.getAllTablespaceNames()) {
      assertTrue(tablespaceName + " does not exist.", catalog.existTablespace(tablespaceName));
      tablespaceProtos.add(catalog.getTablespace(tablespaceName));
    }
    assertEquals(tablespaceProtos, new HashSet<>(catalog.getAllTablespaces()));
  }

  @Test
  public void testDatabase() throws Exception {
    assertTrue(catalog.existDatabase("TestDatabase1"));
    assertTrue(catalog.existDatabase("testDatabase1"));
    assertTrue(catalog.getAllDatabaseNames().contains("TestDatabase1"));
    assertTrue(catalog.getAllDatabaseNames().contains("testDatabase1"));
  }

  @Test
  public void testTable() throws Exception {

    assertTrue(catalog.existsTable("TestDatabase1", "TestTable1"));
    assertTrue(catalog.existsTable("TestDatabase1", "testTable1"));

    Map<String, TableDesc> tableDescs = new HashMap<>();
    for (String eachTableName : catalog.getAllTableNames("TestDatabase1")) {
      TableDesc desc = catalog.getTableDesc("TestDatabase1", eachTableName);
      tableDescs.put(desc.getName(), desc);
    }
    for (TableDescriptorProto eachTableDescriptor : catalog.getAllTables()) {
      String qualifiedTableName = CatalogUtil.buildFQName("TestDatabase1", eachTableDescriptor.getName());
      assertTrue(tableDescs.containsKey(qualifiedTableName));
      TableDesc desc = tableDescs.get(qualifiedTableName);
      assertEquals(desc.getUri().toString(), eachTableDescriptor.getPath());
      assertEquals(desc.getMeta().getStoreType(), eachTableDescriptor.getStoreType());
    }

    // get all column

    // table stats

  }

  @Test
  public void testTablePartition() throws Exception {
    assertTrue(catalog.existsTable("TestDatabase1", "TestPartition1"));
    assertTrue(catalog.existsTable("TestDatabase1", "testPartition1"));

    String partitionName = "DaTe=bBb/dAtE=AaA";
    PartitionDesc partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    catalog.alterTable(alterTableDesc);

    PartitionDescProto resultDesc = catalog.getPartition("TestDatabase1", "TestPartition1",
        partitionName);

    assertNotNull(resultDesc);
    assertEquals(resultDesc.getPartitionName(), partitionName);
    assertEquals(resultDesc.getPath(), "hdfs://xxx.com/warehouse/" + partitionName);
    assertEquals(resultDesc.getPartitionKeysCount(), 2);

    partitionName = "DaTe=BbB/dAtE=aAa";
    partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_PARTITION);

    catalog.alterTable(alterTableDesc);

    resultDesc = catalog.getPartition("TestDatabase1", "TestPartition1",
        partitionName);

    assertNotNull(resultDesc);
    assertEquals(resultDesc.getPartitionName(), partitionName);
    assertEquals(resultDesc.getPath(), "hdfs://xxx.com/warehouse/" + partitionName);
    assertEquals(resultDesc.getPartitionKeysCount(), 2);

    partitionName = "DaTe=BbB/dAtE=aAa";
    partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.DROP_PARTITION);

    catalog.alterTable(alterTableDesc);

    partitionName = "DaTe=bBb/dAtE=AaA";
    partitionDesc = CatalogTestingUtil.buildPartitionDesc(partitionName);

    alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(CatalogUtil.buildFQName("TestDatabase1", "TestPartition1"));
    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(AlterTableType.DROP_PARTITION);

    catalog.alterTable(alterTableDesc);
  }

  @Test
  public void testTableColumn() {

  }

  @Test
  public void testTableProperty() {

  }

  @Test
  public void testIndex() {

  }

  @Test
  public void testFunction() {

  }
}
