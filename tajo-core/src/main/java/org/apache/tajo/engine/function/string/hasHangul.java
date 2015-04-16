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

package org.apache.tajo.engine.function.string;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.BooleanDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

/**
 *
 */
@Description(
    functionName = "has_hangul",
    description = "has hangul",
    detail = "has hangul.",
    example = "> SELECT has_hangul('테스트');\n" + "true",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT})}
)
public class hasHangul extends GeneralFunction {

  public hasHangul() {
    super(new Column[] {new Column("text", TajoDataTypes.Type.TEXT)});
  }

  @Override
  public Datum eval(Tuple params) {
    Datum in = params.get(0);
    if (in == null || in.isNull()) {
      return NullDatum.get();
    }

    TextDatum textDatum = (TextDatum) in;
    String str = textDatum.asChars();

    for (int i=0; i<str.length();i++){
      if (IsHangul(str.charAt(i))){
        return BooleanDatum.TRUE;
      }
    }
    return BooleanDatum.FALSE;
  }

  private boolean IsHangul(char c){
    return ((c >= 0xAC00) && (c <= 0xD7A3));
  }
}
