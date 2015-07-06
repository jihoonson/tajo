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

package org.apache.tajo.storage.text;

import io.netty.buffer.ByteBuf;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.util.Arrays;

public class KernLogDeserializer extends TextLineDeserializer {

  private int [] targetColumnIndexes;

  public KernLogDeserializer(Schema schema, TableMeta meta, Column[] projected) {
    super(schema, meta);

    targetColumnIndexes = new int[projected.length];
    for (int i = 0; i < projected.length; i++) {
      targetColumnIndexes[i] = schema.getColumnId(projected[i].getQualifiedName());
    }
    Arrays.sort(targetColumnIndexes);
  }

  @Override
  public void init() {

  }

  @Override
  public void deserialize(final ByteBuf lineBuf, Tuple output) throws IOException, TextLineParsingError {
    byte[] bytes = new byte[lineBuf.readableBytes()];
    lineBuf.readBytes(bytes);
    String line = new String(bytes);
    String[] tokens = line.split("\\s+");
    Datum[] datums = new Datum[schema.size()];
    datums[0] = DatumFactory.createTimestamp("2015 " + tokens[0] + " " + tokens[1] + " " + tokens[2]);
    datums[1] = DatumFactory.createText(tokens[3]);
    datums[2] = DatumFactory.createText(tokens[4]);
    if (tokens[5].length()-1 >= 1) {
      datums[3] = DatumFactory.createFloat8(tokens[5].substring(1, tokens[5].length() - 1));
    } else {
      datums[3] = NullDatum.get();
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 6; i < tokens.length; i++) {
      sb.append(tokens[i]);
    }
    datums[4] = DatumFactory.createText(sb.toString());

    Datum[] projectedDatums = new Datum[targetColumnIndexes.length];
    int i = 0;
    for (Integer targetColumn : targetColumnIndexes) {
      projectedDatums[i++] = datums[targetColumn];
    }

    output.put(projectedDatums);
  }

  @Override
  public void release() {

  }
}
