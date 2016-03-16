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

package org.apache.tajo.storage.orc2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

public class SchemaEvolution {

  private static final Log LOG = LogFactory.getLog(SchemaEvolution.class);

  public static TreeReaderFactory.TreeReaderSchema validateAndCreate(List<org.apache.orc.OrcProto.Type> fileTypes,
                                                                     List<org.apache.orc.OrcProto.Type> schemaTypes) throws IOException {

    final List<org.apache.orc.OrcProto.Type> rowSchema;
    int rowSubtype;
    rowSubtype = 0;
    rowSchema = fileTypes;

    // Do checking on the overlap.  Additional columns will be defaulted to NULL.

    int numFileColumns = rowSchema.get(0).getSubtypesCount();
    int numDesiredColumns = schemaTypes.get(0).getSubtypesCount();

    int numReadColumns = Math.min(numFileColumns, numDesiredColumns);

    /**
     * Check type promotion.
     *
     * Currently, we only support integer type promotions that can be done "implicitly".
     * That is, we know that using a bigger integer tree reader on the original smaller integer
     * column will "just work".
     *
     * In the future, other type promotions might require type conversion.
     */
    // short -> int -> bigint as same integer readers are used for the above types.

    for (int i = 0; i < numReadColumns; i++) {
      org.apache.orc.OrcProto.Type fColType = fileTypes.get(rowSubtype + i);
      org.apache.orc.OrcProto.Type rColType = schemaTypes.get(i);
      if (!fColType.getKind().equals(rColType.getKind())) {

        boolean ok = false;
        if (fColType.getKind().equals(org.apache.orc.OrcProto.Type.Kind.SHORT)) {

          if (rColType.getKind().equals(org.apache.orc.OrcProto.Type.Kind.INT) ||
              rColType.getKind().equals(org.apache.orc.OrcProto.Type.Kind.LONG)) {
            // type promotion possible, converting SHORT to INT/LONG requested type
            ok = true;
          }
        } else if (fColType.getKind().equals(org.apache.orc.OrcProto.Type.Kind.INT)) {

          if (rColType.getKind().equals(org.apache.orc.OrcProto.Type.Kind.LONG)) {
            // type promotion possible, converting INT to LONG requested type
            ok = true;
          }
        }

        if (!ok) {
          throw new IOException("ORC does not support type conversion from " +
              fColType.getKind().name() + " to " + rColType.getKind().name());
        }
      }
    }

    List<org.apache.orc.OrcProto.Type> fullSchemaTypes = schemaTypes;
    int innerStructSubtype = rowSubtype;

    return new TreeReaderFactory.TreeReaderSchema().
        fileTypes(fileTypes).
        schemaTypes(fullSchemaTypes).
        innerStructSubtype(innerStructSubtype);
  }
}
