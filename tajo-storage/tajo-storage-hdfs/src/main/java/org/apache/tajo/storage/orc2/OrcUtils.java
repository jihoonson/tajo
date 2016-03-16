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

import com.google.common.collect.Lists;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SnappyCodec;
import org.apache.orc.impl.ZlibCodec;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedDataTypeException;

import java.util.List;

public class OrcUtils {

  public static List<OrcProto.Type> getOrcTypes(Column[] columns) {
    List<OrcProto.Type> result = Lists.newArrayList();
    appendOrcTypes(result, columns);
    return result;
  }

  private static void appendOrcTypes(List<OrcProto.Type> result, Column[] columns) {
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();

    for (Column col : columns) {
      switch (col.getTypeDesc().getDataType().getType()) {
        case BOOLEAN:
          type.setKind(OrcProto.Type.Kind.BOOLEAN);
          break;
        case INT1:
        case INT2:
          type.setKind(OrcProto.Type.Kind.SHORT);
          break;
        case INT4:
          type.setKind(OrcProto.Type.Kind.INT);
          break;
        case INT8:
          type.setKind(OrcProto.Type.Kind.LONG);
          break;
        case FLOAT4:
          type.setKind(OrcProto.Type.Kind.FLOAT);
          break;
        case FLOAT8:
          type.setKind(OrcProto.Type.Kind.DOUBLE);
          break;
        case CHAR:
          type.setKind(OrcProto.Type.Kind.CHAR);
          type.setMaximumLength(col.getDataType().getLength());
          break;
        case TEXT:
          type.setKind(OrcProto.Type.Kind.STRING);
          break;
        case DATE:
          type.setKind(OrcProto.Type.Kind.DATE);
          break;
        case TIMESTAMP:
          type.setKind(OrcProto.Type.Kind.TIMESTAMP);
          break;
        case INTERVAL:
          break;
        case BIT:
          type.setKind(OrcProto.Type.Kind.BYTE);
          break;
        case BLOB:
          type.setKind(OrcProto.Type.Kind.BINARY);
          break;
        case RECORD:
          // TODO
          break;
        case MAP:
          break;
        case BOOLEAN_ARRAY:
        case INT1_ARRAY:
        case INT2_ARRAY:
        case INT4_ARRAY:
        case INT8_ARRAY:
        case FLOAT4_ARRAY:
        case FLOAT8_ARRAY:
        case CHAR_ARRAY:
        case TEXT_ARRAY:
        case DATE_ARRAY:
        case TIME_ARRAY:
        case TIMESTAMP_ARRAY:
        case INTERVAL_ARRAY:
          // TODO
          break;
        default:
          throw new TajoRuntimeException(new UnsupportedDataTypeException(col.getDataType().getType().name()));
      }
      result.add(type.build());
    }
  }

  public static CompressionCodec createCodec(CompressionKind kind) {
    switch (kind) {
      case NONE:
        return null;
      case ZLIB:
        return new ZlibCodec();
      case SNAPPY:
        return new SnappyCodec();
      case LZO:
        try {
          ClassLoader loader = Thread.currentThread().getContextClassLoader();
          if (loader == null) {
            throw new RuntimeException("error while getting a class loader");
          }
          @SuppressWarnings("unchecked")
          Class<? extends CompressionCodec> lzo =
              (Class<? extends CompressionCodec>)
                  loader.loadClass("org.apache.hadoop.hive.ql.io.orc.LzoCodec");
          return lzo.newInstance();
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("LZO is not available.", e);
        } catch (InstantiationException e) {
          throw new IllegalArgumentException("Problem initializing LZO", e);
        } catch (IllegalAccessException e) {
          throw new IllegalArgumentException("Insufficient access to LZO", e);
        }
      default:
        throw new IllegalArgumentException("Unknown compression codec: " +
            kind);
    }
  }
}
