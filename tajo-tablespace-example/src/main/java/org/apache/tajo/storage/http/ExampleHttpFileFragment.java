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

package org.apache.tajo.storage.http;

import org.apache.tajo.storage.fragment.BuiltinFragmentKinds;
import org.apache.tajo.storage.fragment.Fragment;

import java.net.URI;

public class ExampleHttpFileFragment extends Fragment<Long> {

  private final String tempDir;
  private final boolean cleanOnExit;

  /**
   *
   * @param uri
   * @param inputSourceId
   * @param startKey first byte pos
   * @param endKey last byte pos
   * @param tempDir
   * @param cleanOnExit
   */
  public ExampleHttpFileFragment(URI uri,
                                 String inputSourceId,
                                 long startKey,
                                 long endKey,
                                 String tempDir,
                                 boolean cleanOnExit) {
    super(BuiltinFragmentKinds.HTTP, uri, inputSourceId, startKey, endKey, endKey - startKey, null);
    this.tempDir = tempDir;
    this.cleanOnExit = cleanOnExit;
  }

  public boolean cleanOnExit() {
    return cleanOnExit;
  }

  public String getTempDir() {
    return tempDir;
  }
}
