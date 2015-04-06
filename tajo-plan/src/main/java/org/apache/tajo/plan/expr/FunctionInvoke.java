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
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public abstract class FunctionInvoke {
  @Expose protected FunctionDesc functionDesc;

  public FunctionInvoke(FunctionDesc functionDesc) {
    this.functionDesc = functionDesc;
  }

  public static FunctionInvoke newInstance(FunctionDesc desc) throws InternalException {
    if (desc.getInvocation().hasLegacy()) {
      return new LegacyScalarFunctionInvoke(desc);
    } else if (desc.getInvocation().hasPython()) {
//      return new PythonFunctionInvoke(desc);
      return new PythonFunctionInvoke2(desc);
    } else {
      throw new UnsupportedException(desc.getInvocation() + " is not supported");
    }
  }

  public abstract void init(FunctionInvokeContext invokeContext, FunctionEval.ParamType[] paramTypes) throws IOException;


//  /**
//   * Bind an input schema.
//   * @param schema intpu schema
//   */
//  public abstract void bind(Schema schema);

  /**
   * Evaluate the given tuple with a function
   * @param tuple a tuple evaluated with parameters
   * @return a result of a fuction execution
   */
  public abstract Datum eval(Tuple tuple);

  @Override
  public boolean equals(Object o) {
    if (o instanceof FunctionInvoke) {
      FunctionInvoke other = (FunctionInvoke) o;
      return this.functionDesc.equals(other.functionDesc);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return functionDesc.hashCode();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    FunctionInvoke clone = (FunctionInvoke) super.clone();
    clone.functionDesc = (FunctionDesc) this.functionDesc.clone();
    return clone;
  }
}