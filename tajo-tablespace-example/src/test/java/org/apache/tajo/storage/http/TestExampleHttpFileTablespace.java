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

import io.netty.handler.codec.http.HttpHeaders.Names;
import net.minidev.json.JSONObject;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.List;

public class TestExampleHttpFileTablespace {

  private static ExampleHttpTablespaceTestServer server;
  private static final TpchTestBase testBase = TpchTestBase.getInstance();
  private static final TajoTestingCluster testingCluster = testBase.getTestingCluster();
  private static TajoClient client;

  @BeforeClass
  public static void setup() throws Exception {
    server = new ExampleHttpTablespaceTestServer(true);
    server.init();

    JSONObject configElements = new JSONObject();
    URI uri = URI.create("http://" + InetAddress.getLocalHost().getHostName() + ":" + server.getAddress().getPort());
    TablespaceManager.addTableSpaceForTest(new ExampleHttpFileTablespace("http_example", uri, configElements));

    testingCluster.getMaster().refresh();

    client = testingCluster.newTajoClient();
  }

  @AfterClass
  public static void teardown() throws IOException {
    client.close();
    testingCluster.shutdownMiniCluster();
    server.close();
  }

  @Test
  public void testSpace() throws TajoException, IOException {
    client.executeQuery("create database test");
    client.selectDatabase("test");
    client.executeQuery("create table nation (N_NATIONKEY bigint, N_NAME text, N_REGIONKEY bigint, N_COMMENT text) tablespace http_example with ('path'='lineitem.tbl')");
    TableDesc desc = client.getTableDesc("nation");
    System.out.println(desc.toString());
    List<Fragment> fragments = TablespaceManager.get(desc.getUri()).getSplits("nation", desc, false, null);
    System.out.println(fragments.size());
    for (Fragment f : fragments) {
      System.out.println(f.getUri() + " (" + f.getStartKey() + ", " + f.getEndKey() + ")");
    }

    client.executeQuery("select * from nation");
  }

  @Test
  public void testTest() throws InterruptedException, IOException {
    ExampleHttpTablespaceTestServer server = new ExampleHttpTablespaceTestServer(true);
    server.init();
    System.out.println(server.getAddress());

    InetSocketAddress address = server.getAddress();
    String url = "http://" + InetAddress.getLocalHost().getHostName() + ":" + address.getPort() + "/lineitem.tbl";
    System.out.println(url);

    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("HEAD");
    connection.connect();
    long contentLength = connection.getHeaderFieldLong("Content-Length", -1);
    System.out.println(contentLength);
    connection.disconnect();

    connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("HEAD");
    connection.setRequestProperty("Range", "bytes=0-");
    connection.connect();
    System.out.println(connection.getResponseCode());
    connection.disconnect();

    connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Range", "bytes=0-");
    connection.connect();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    InputStream in = connection.getInputStream();
    while (in.available() > 0) {
      baos.write(in.read());
    }
    in.close();

    System.out.println(baos.size());
    System.out.println(baos.toString());
    baos.close();
    connection.disconnect();

    connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Range", "bytes=100-");
    connection.connect();
    baos = new ByteArrayOutputStream();
    in = connection.getInputStream();
    while (in.available() > 0) {
      baos.write(in.read());
    }
    in.close();

    System.out.println(baos.size());
    System.out.println(baos.toString());
    baos.close();
    connection.disconnect();

    server.close();
  }

  @Test
  public void testTeeest() throws IOException {
//    String uri = "https://jihoonson.files.wordpress.com/2016/04/flamegraph-tim3.png?w=605";
    String uri = "http://localhost/flamegraph-radix.svg";
    URL url = new URL(uri);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("HEAD");
    connection.setRequestProperty(Names.RANGE, "bytes=10-99");
    connection.connect();
    System.out.println(connection.getHeaderFieldLong("Content-Length", -1));
    System.out.println(connection.getResponseCode());
    connection.disconnect();

//    connection = (HttpURLConnection) url.openConnection();
//    connection.setRequestMethod("GET");
//    connection.setRequestProperty(Names.RANGE, "bytes=10-99");
//    connection.connect();
//    ReadableByteChannel rbc = Channels.newChannel(connection.getInputStream());
//    FileOutputStream fos = new FileOutputStream("/tmp/tajo-jihoon/test.png");
//    FileChannel fc = fos.getChannel();
//    fc.transferFrom(rbc, 0, Long.MAX_VALUE);
//    fc.close();
//    fos.close();
//    rbc.close();
//    connection.disconnect();

//    ReadableByteChannel rbc = Channels.newChannel(url.openStream());
//    FileOutputStream fos = new FileOutputStream("/tmp/tajo-jihoon/test.png");
//    FileChannel fc = fos.getChannel();
//    fc.transferFrom(rbc, 0, Long.MAX_VALUE);
//    fc.close();
//    fos.close();
//    rbc.close();
  }
}
