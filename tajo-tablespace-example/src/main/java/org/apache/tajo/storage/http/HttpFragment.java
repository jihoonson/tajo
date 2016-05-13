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

import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.storage.fragment.Fragment;

import java.net.URI;

public class HttpFragment implements Fragment {
  private final URI uri;
  private final String tableName;
  private final HttpFragmentKey startKey;
  private final HttpFragmentKey endKey;
  private final String[] hosts;

  public HttpFragment(URI uri, String tableName, long startKey, long endKey, String[] hosts) {
    this.uri = uri;
    this.tableName = tableName;
    this.startKey = new HttpFragmentKey(startKey);
    this.endKey = new HttpFragmentKey(endKey);
    this.hosts = hosts;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public FragmentProto getProto() {
    return null;
  }

  @Override
  public HttpFragmentKey getStartKey() {
    return startKey;
  }

  @Override
  public HttpFragmentKey getEndKey() {
    return endKey;
  }

  @Override
  public long getLength() {
    return endKey.getKey() - startKey.getKey();
  }

  @Override
  public String[] getHosts() {
    return hosts;
  }

  @Override
  public boolean isEmpty() {
    return endKey.getKey() - startKey.getKey() == 0;
  }
}
