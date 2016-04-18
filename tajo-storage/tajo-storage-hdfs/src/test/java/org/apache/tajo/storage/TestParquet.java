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

package org.apache.tajo.storage;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.fragment.FileFragment;
import org.junit.Test;

import java.io.IOException;

public class TestParquet {
  private final static String PARQUET_PATH = "hdfs://localhost:7020/parquet.dat";
  private final static TajoConf conf = new TajoConf();
  private final static Schema schema = SchemaFactory.newV1(new Column[] {
      new Column("id", Type.INT4)
  });

  private final static TableMeta meta = CatalogUtil.newTableMeta(BuiltinStorages.PARQUET,
      CatalogUtil.newDefaultProperty(BuiltinStorages.PARQUET));

  @Test
  public void testWrite() throws IOException {
    Path tablePath = new Path(PARQUET_PATH);
    FileTablespace tablespace = TablespaceManager.get("hdfs://localhost:7020");
    Appender appender = tablespace.getAppender(meta, schema, tablePath);
    appender.init();
    Tuple tuple = new VTuple(1);
    tuple.put(0, DatumFactory.createInt4(234));
    appender.addTuple(tuple);
    tuple.put(0, DatumFactory.createInt4(123));
    appender.addTuple(tuple);
    appender.close();
  }

  @Test
  public void testRead() throws IOException {
    Path tablePath = new Path(PARQUET_PATH);
    FileTablespace tablespace = TablespaceManager.get("hdfs://localhost:7020");
    FileSystem fs = tablePath.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(tablePath);
    FileFragment fragment = new FileFragment("test", tablePath, 0, fileStatus.getLen());
    Scanner scanner = tablespace.getScanner(meta, schema, fragment, schema);
    scanner.init();
    System.out.println(scanner.next());
    System.out.println(scanner.next());
    scanner.close();
  }
}
