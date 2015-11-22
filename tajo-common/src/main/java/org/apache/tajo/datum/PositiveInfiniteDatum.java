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

package org.apache.tajo.datum;

import org.apache.tajo.common.TajoDataTypes.Type;

public class PositiveInfiniteDatum extends Datum {
  private static PositiveInfiniteDatum instance;

  static {
    instance = new PositiveInfiniteDatum();
  }

  public static PositiveInfiniteDatum get() {
    return instance;
  }

  private PositiveInfiniteDatum() {
    super(Type.POSITIVE_INFINITE);
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int compareTo(Datum datum) {
    return 1;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PositiveInfiniteDatum;
  }

  @Override
  public int hashCode() {
    return Integer.MAX_VALUE;
  }

  @Override
  public String toString() {
    return "+INF";
  }
}
