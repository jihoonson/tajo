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
import io.netty.handler.codec.http.HttpMethod;
import net.minidev.json.JSONObject;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.sql.ResultSet;
import java.util.List;

public class TestExampleHttpFileTablespace {

//  private final static String DATABASE_NAME = TestExampleHttpFileTablespace.class.getSimpleName().toLowerCase();
//
//  private static ExampleHttpTablespaceTestServer server;
//  private static final TpchTestBase testBase = TpchTestBase.getInstance();
//  private static final TajoTestingCluster testingCluster = testBase.getTestingCluster();
//  private static TajoClient client;

  @BeforeClass
  public static void setup() throws Exception {
//    server = new ExampleHttpTablespaceTestServer();
//    server.init();
//
//    JSONObject configElements = new JSONObject();
//    URI uri = URI.create("http://" + InetAddress.getLocalHost().getHostName() + ":" + server.getAddress().getPort());
//    TablespaceManager.addTableSpaceForTest(new ExampleGithubRestTablespace("http_example", uri, configElements));
//
//    testingCluster.getMaster().refresh();
//
//    client = testingCluster.newTajoClient();
//
//    client.executeQuery("create database " + DATABASE_NAME);
//    client.selectDatabase(DATABASE_NAME);
//    client.executeQuery("create table got (*) tablespace http_example using ex_http_json with ('path'='got.json')");
  }

  @AfterClass
  public static void teardown() throws Exception {
//    client.dropTable("got");
//    client.selectDatabase("default");
//    client.dropDatabase(DATABASE_NAME);
//    client.close();
//    testingCluster.shutdownMiniCluster();
//    server.close();
  }

  @Test
  public void testSpace() throws Exception {
//    TableDesc desc = client.getTableDesc(DATABASE_NAME + ".got");
//    System.out.println(desc.toString());
//    List<Fragment> fragments = TablespaceManager.get(desc.getUri()).getSplits(desc.getName(), desc, false, null);
//    System.out.println(fragments.size());
//    for (Fragment f : fragments) {
//      System.out.println(f.getUri() + " (" + f.getStartKey() + ", " + f.getEndKey() + ")");
//    }
//
//    ResultSet res = testBase.execute("select count(title) from " + DATABASE_NAME + ".got");
//    System.out.println(QueryTestCaseBase.resultSetToString(res));
  }

  @Test
  public void testTablespaceHandler() throws IOException {
    String str = "https://api.github.com/users/jihoonson/events" +
        "?page=1&per_page=1&" +
        "client_id=" +
        "6b93054d1f2b57b93e09" +
        "&" +
        "client_secret=" +
        "d6bce3d380702f2d9adb917fc97bcf3e04cc1a03";
    System.out.println(str);
    URL url = new URL(str);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();

    connection.setRequestMethod(HttpMethod.HEAD.name());
    connection.setRequestProperty(Names.CONTENT_LENGTH, null);

    connection.connect();

    System.out.println(connection.getResponseCode());
    System.out.println(connection.getHeaderFieldLong(Names.CONTENT_LENGTH, -1));
    System.out.println(connection.getHeaderField("Link"));

//    DataInputStream dis = new DataInputStream(connection.getInputStream());
//    while (dis.available() > 0) {
//      String line = dis.readLine();
//      System.out.println(line);
//      System.out.println(line.length());
//    }

    connection.disconnect();
  }

  @Test
  public void testGetTableVolume() {

  }
}
