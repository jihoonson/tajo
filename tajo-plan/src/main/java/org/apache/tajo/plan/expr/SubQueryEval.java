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

package org.apache.tajo.plan.expr;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

public class SubQueryEval extends ValueSetEval {
  @Expose String subQueryBlockName;

  public SubQueryEval(String subQueryBlockName) {
    super(EvalType.SUB_QUERY);
    this.subQueryBlockName = subQueryBlockName;
  }

  public String getSubQueryBlockName() {
    return subQueryBlockName;
  }

  @Override
  public String getName() {
    return "SubQuery";
  }

  @Override
  public Datum eval(Schema schema, Tuple tuple) {
    return NullDatum.get();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SubQueryEval) {
      SubQueryEval other = (SubQueryEval) o;
      return this.dataType.equals(other.dataType) &&
          this.subQueryBlockName.equals(other.subQueryBlockName);
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SubQueryEval clone = (SubQueryEval) super.clone();
    clone.dataType = CatalogUtil.newSimpleDataType(dataType.getType());
    clone.subQueryBlockName = new String(subQueryBlockName);
    return clone;
  }

  @Override
  public Datum[] getValues() {
    throw new UnsupportedException("SubQueryEval does not support the getValues() function");
  }
}
