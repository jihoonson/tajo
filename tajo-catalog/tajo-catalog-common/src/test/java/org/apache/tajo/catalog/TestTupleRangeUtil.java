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

import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

public class TestTupleRangeUtil {

  public void testComputeCardinality() {
    Tuple start = new VTuple(13);
    Tuple end = new VTuple(13);

    SortSpec[] sortSpecs = new SortSpec[13];
    sortSpecs[0] = new SortSpec(new Column("col1", Type.BOOLEAN));
    sortSpecs[1] = new SortSpec(new Column("col2", Type.CHAR), false, false);
//    sortSpecs[2] = new SortSpec(new Column("col3", Type.BIT), )

    start.put(0, DatumFactory.createBool(false));
  }
}
