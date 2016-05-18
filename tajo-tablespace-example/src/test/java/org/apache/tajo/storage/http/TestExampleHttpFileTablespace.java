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

import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;

public class TestExampleHttpFileTablespace {

  @Test
  public void testTest() throws InterruptedException, IOException {
    ExampleHttpTablespaceTestServer server = new ExampleHttpTablespaceTestServer(true);
    server.init();
    System.out.println(server.getAddress());

    InetSocketAddress address = server.getAddress();
    String url = "http://localhost:" + address.getPort() + "/test";
    System.out.println(url);

    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("HEAD");
//    connection.setRequestProperty("Content-Length", "0");
    connection.connect();
    System.out.println(connection.getHeaderFieldLong("Content-Length", -1));
    connection.disconnect();

    connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("HEAD");
    connection.setRequestProperty("Range", "bytes:0-");
    connection.connect();
    System.out.println(connection.getResponseCode());
    connection.disconnect();

    server.close();
  }

  @Test
  public void testTeeest() throws IOException {
    String uri = "https://jihoonson.files.wordpress.com/2016/04/flamegraph-tim3.png?w=605";
    URL url = new URL(uri);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("HEAD");
//    connection.setRequestProperty("Content-Length", "0");
    connection.connect();
    System.out.println(connection.getHeaderFieldLong("Content-Length", -1));
    connection.disconnect();

    connection = (HttpURLConnection) url.openConnection();
    System.out.println(connection.getContentLengthLong());
    connection.disconnect();

//    ReadableByteChannel rbc = Channels.newChannel(url.openStream());
//    FileOutputStream fos = new FileOutputStream("/tmp/tajo-jihoon/test.png");
//    FileChannel fc = fos.getChannel();
//    fc.transferFrom(rbc, 0, Long.MAX_VALUE);
//    fc.close();
//    fos.close();
//    rbc.close();
  }
}
