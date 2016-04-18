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

package org.apache.tajo.worker;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoProtos.FetcherState;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaFactory;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.pullserver.PullServerConstants;
import org.apache.tajo.pullserver.PullServerUtil;
import org.apache.tajo.pullserver.PullServerUtil.PullServerRequestURIBuilder;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexWriter;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.worker.FetchImpl.RangeParam;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestFetcher {
  enum FetchType {
    LOCAL,
    REMOTE
  }

  private String TEST_DATA = TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestFetcher";
  private String INPUT_DIR = TEST_DATA+"/in/";
  private String OUTPUT_DIR = TEST_DATA+"/out/";
  private TajoConf conf = new TajoConf();
  private TajoPullServerService pullServerService;
  private final FetchType type;
  private final int maxUrlLength = conf.getIntVar(ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH);

  public TestFetcher(FetchType type) {
    this.type = type;
  }

  @Before
  public void setUp() throws Exception {
    CommonTestingUtil.getTestDir(TEST_DATA);
    CommonTestingUtil.getTestDir(INPUT_DIR);
    CommonTestingUtil.getTestDir(OUTPUT_DIR);
    conf.setVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR, INPUT_DIR);
//    conf.setIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_READ_TIMEOUT, 1);
    conf.setIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE, 127);

    pullServerService = new TajoPullServerService();
    pullServerService.init(conf);
    pullServerService.start();
  }

  @After
  public void tearDown() {
    pullServerService.stop();
  }

  @Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][] {
        {FetchType.LOCAL},
        {FetchType.REMOTE}
    });
  }

  @Test
  public void testGetHashShuffle() throws IOException {
    Random rnd = new Random();
    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String partId = "1";

    Path queryBaseDir = PullServerUtil.getBaseOutputDir(queryId.toString(), sid);
    final int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
    final Path dataPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);

    PullServerRequestURIBuilder builder = new PullServerRequestURIBuilder("127.0.0.1", pullServerService.getPort(),
        maxUrlLength);
    builder.setRequestType(PullServerConstants.CHUNK_REQUEST_PARAM_STRING)
        .setQueryId(queryId.toString())
        .setEbId(sid)
        .setPartId(partId)
        .setShuffleType(PullServerConstants.HASH_SHUFFLE_PARAM_STRING);

    Path inputPath = new Path(INPUT_DIR, dataPath);
    FSDataOutputStream stream = FileSystem.getLocal(conf).create(inputPath, true);
    for (int i = 0; i < 100; i++) {
      String data = ""+rnd.nextInt();
      stream.write(data.getBytes());
    }
    stream.flush();
    stream.close();

    URI uri = builder.build(false).get(0);
    File data = new File(OUTPUT_DIR + "data");

    final AbstractFetcher fetcher;
    if (type.equals(FetchType.LOCAL)) {
      fetcher = new LocalFetcher(conf, uri, "test");
    } else {
      FileChunk storeChunk = new FileChunk(data, 0, data.length());
      storeChunk.setFromRemote(true);
      fetcher = new RemoteFetcher(conf, uri, storeChunk);
    }

    FileChunk chunk = fetcher.get().get(0);
    assertNotNull(chunk);
    assertNotNull(chunk.getFile());

    FileSystem fs = FileSystem.getLocal(new TajoConf());
    FileStatus inStatus = fs.getFileStatus(inputPath);
    FileStatus outStatus = fs.getFileStatus(new Path(chunk.getFile().getAbsolutePath()));

    assertEquals(inStatus.getLen(), outStatus.getLen());
    assertEquals(TajoProtos.FetcherState.FETCH_FINISHED, fetcher.getState());
  }

  @Test
  public void testGetRangeShuffle() throws IOException {
    Random rnd = new Random();
    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String partId = "1";
    String taskId = "1";
    String attemptId = "0";

    Path queryBaseDir = PullServerUtil.getBaseOutputDir(queryId.toString(), sid);
    Path outDir = StorageUtil.concatPath(queryBaseDir, taskId + "_" + attemptId, "output");
    Path dataPath = StorageUtil.concatPath(outDir, "output");
    Path indexPath = StorageUtil.concatPath(outDir, "index");

    List<String> strings = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      strings.add("" + rnd.nextInt());
    }
    Collections.sort(strings);

    Path inputPath = new Path(INPUT_DIR, dataPath);
    FileSystem fs = FileSystem.getLocal(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    final FSDataOutputStream stream = fs.create(inputPath, true);
    BSTIndex index = new BSTIndex(conf);
    Schema schema = SchemaFactory.newV1(new Column[] {new Column("rnd", Type.TEXT)});
    SortSpec[] sortSpecs = new SortSpec[] {new SortSpec(schema.getColumn(0))};
    BSTIndexWriter writer = index.getIndexWriter(new Path(INPUT_DIR, indexPath), BSTIndex.TWO_LEVEL_INDEX, schema, new BaseTupleComparator(schema, sortSpecs), true);
    writer.init();

    for (String t : strings) {
      stream.write(t.getBytes());
      writer.write(new VTuple(new Datum[] {DatumFactory.createText(t)}), stream.getPos());
    }
    stream.flush();
    writer.flush();
    stream.close();
    writer.close();

    RangeParam rangeParam = new RangeParam(new TupleRange(sortSpecs,
        new VTuple(new Datum[] {DatumFactory.createText(strings.get(0))}),
        new VTuple(new Datum[] {DatumFactory.createText(strings.get(strings.size() - 1))})), true, RowStoreUtil.createEncoder(schema));
    PullServerRequestURIBuilder builder = new PullServerRequestURIBuilder("127.0.0.1", pullServerService.getPort(),
        maxUrlLength);
    builder.setRequestType(PullServerConstants.CHUNK_REQUEST_PARAM_STRING)
        .setQueryId(queryId.toString())
        .setEbId(sid)
        .setPartId(partId)
        .setShuffleType(PullServerConstants.RANGE_SHUFFLE_PARAM_STRING)
        .setTaskIds(Lists.newArrayList(Integer.parseInt(taskId)))
        .setAttemptIds(Lists.newArrayList(Integer.parseInt(attemptId)))
        .setStartKeyBase64(new String(Base64.encodeBase64(rangeParam.getStart())))
        .setEndKeyBase64(new String(Base64.encodeBase64(rangeParam.getEnd())))
        .setLast(true);

    URI uri = builder.build(true).get(0);
    File data = new File(OUTPUT_DIR + "data");

    final AbstractFetcher fetcher;
    if (type.equals(FetchType.LOCAL)) {
      fetcher = new LocalFetcher(conf, uri, "test");
    } else {
      FileChunk storeChunk = new FileChunk(data, 0, data.length());
      storeChunk.setFromRemote(true);
      fetcher = new RemoteFetcher(conf, uri, storeChunk);
    }

    FileChunk chunk = fetcher.get().get(0);
    assertNotNull(chunk);
    assertNotNull(chunk.getFile());

    FileStatus inStatus = fs.getFileStatus(inputPath);
    FileStatus outStatus = fs.getFileStatus(new Path(chunk.getFile().getAbsolutePath()));

    assertEquals(inStatus.getLen(), outStatus.getLen());
    assertEquals(TajoProtos.FetcherState.FETCH_FINISHED, fetcher.getState());
  }

  @Test
  public void testAdjustFetchProcess() {
    assertEquals(0.0f, TaskImpl.adjustFetchProcess(0, 0), 0);
    assertEquals(0.0f, TaskImpl.adjustFetchProcess(10, 10), 0);
    assertEquals(0.05f, TaskImpl.adjustFetchProcess(10, 9), 0);
    assertEquals(0.1f, TaskImpl.adjustFetchProcess(10, 8), 0);
    assertEquals(0.25f, TaskImpl.adjustFetchProcess(10, 5), 0);
    assertEquals(0.45f, TaskImpl.adjustFetchProcess(10, 1), 0);
    assertEquals(0.5f, TaskImpl.adjustFetchProcess(10, 0), 0);
  }

  @Test
  public void testStatus() throws Exception {
    Random rnd = new Random();
    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String ta = "1_0";
    String partId = "1";

    Path queryBaseDir = PullServerUtil.getBaseOutputDir(queryId.toString(), sid);
    final int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
    final Path dataPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);

    PullServerRequestURIBuilder builder = new PullServerRequestURIBuilder("127.0.0.1", pullServerService.getPort(),
        maxUrlLength);
    builder.setRequestType(PullServerConstants.CHUNK_REQUEST_PARAM_STRING)
        .setQueryId(queryId.toString())
        .setEbId(sid)
        .setPartId(partId)
        .setShuffleType(PullServerConstants.HASH_SHUFFLE_PARAM_STRING)
        .setTaskAttemptIds(Lists.newArrayList(ta));

    FSDataOutputStream stream =  FileSystem.getLocal(conf).create(new Path(INPUT_DIR, dataPath), true);
    for (int i = 0; i < 100; i++) {
      String data = ""+rnd.nextInt();
      stream.write(data.getBytes());
    }
    stream.flush();
    stream.close();

    URI uri = builder.build(true).get(0);
    File data = new File(OUTPUT_DIR + "data");
    final AbstractFetcher fetcher;
    if (type.equals(FetchType.LOCAL)) {
      fetcher = new LocalFetcher(conf, uri, "test");
    } else {
      FileChunk storeChunk = new FileChunk(data, 0, data.length());
      storeChunk.setFromRemote(true);
      fetcher = new RemoteFetcher(conf, uri, storeChunk);
    }
    assertEquals(TajoProtos.FetcherState.FETCH_INIT, fetcher.getState());

    fetcher.get();
    assertEquals(TajoProtos.FetcherState.FETCH_FINISHED, fetcher.getState());
  }

  @Test
  public void testNoContentFetch() throws Exception {

    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String ta = "1_0";
    String partId = "1";

    Path queryBaseDir = PullServerUtil.getBaseOutputDir(queryId.toString(), sid);
    final int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
    final Path dataPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);

    PullServerRequestURIBuilder builder = new PullServerRequestURIBuilder("127.0.0.1", pullServerService.getPort(),
        maxUrlLength);
    builder.setRequestType(PullServerConstants.CHUNK_REQUEST_PARAM_STRING)
        .setQueryId(queryId.toString())
        .setEbId(sid)
        .setPartId(partId)
        .setShuffleType(PullServerConstants.HASH_SHUFFLE_PARAM_STRING)
        .setTaskAttemptIds(Lists.newArrayList(ta));

    Path inputPath = new Path(INPUT_DIR, dataPath);
    FileSystem fs = FileSystem.getLocal(conf);
    if(fs.exists(inputPath)){
      fs.delete(inputPath, true);
    }

    FSDataOutputStream stream =  fs.create(inputPath, true);
    stream.close();

    URI uri = builder.build(true).get(0);
    File data = new File(OUTPUT_DIR + "data");
    final AbstractFetcher fetcher;
    if (type.equals(FetchType.LOCAL)) {
      fetcher = new LocalFetcher(conf, uri, "test");
    } else {
      FileChunk storeChunk = new FileChunk(data, 0, data.length());
      storeChunk.setFromRemote(true);
      fetcher = new RemoteFetcher(conf, uri, storeChunk);
    }
    assertEquals(TajoProtos.FetcherState.FETCH_INIT, fetcher.getState());

    try {
      fetcher.get();
      if (type.equals(FetchType.LOCAL)) {
        fail();
      }
    } catch (IOException e) {
      if (type.equals(FetchType.REMOTE)) {
        fail();
      }
    }
    assertEquals(FetcherState.FETCH_FAILED, fetcher.getState());
  }

  @Test
  public void testFailureStatus() throws Exception {
    Random rnd = new Random();

    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String ta = "1_0";
    String partId = "1";

    Path queryBaseDir = PullServerUtil.getBaseOutputDir(queryId.toString(), sid);
    final int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
    final Path dataPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);

    PullServerRequestURIBuilder builder = new PullServerRequestURIBuilder("127.0.0.1", pullServerService.getPort(),
        maxUrlLength);
    builder.setRequestType(PullServerConstants.CHUNK_REQUEST_PARAM_STRING)
        .setQueryId(queryId.toString())
        .setEbId(sid)
        .setPartId(partId)
        .setShuffleType("x") //TajoPullServerService will be throws BAD_REQUEST by Unknown shuffle type
        .setTaskAttemptIds(Lists.newArrayList(ta));

    FSDataOutputStream stream =  FileSystem.getLocal(conf).create(new Path(INPUT_DIR, dataPath), true);

    for (int i = 0; i < 100; i++) {
      String data = "" + rnd.nextInt();
      stream.write(data.getBytes());
    }
    stream.flush();
    stream.close();

    URI uri = builder.build(true).get(0);
    File data = new File(OUTPUT_DIR + "data");
    final AbstractFetcher fetcher;
    if (type.equals(FetchType.LOCAL)) {
      fetcher = new LocalFetcher(conf, uri, "test");
    } else {
      FileChunk storeChunk = new FileChunk(data, 0, data.length());
      storeChunk.setFromRemote(true);
      fetcher = new RemoteFetcher(conf, uri, storeChunk);
    }
    assertEquals(TajoProtos.FetcherState.FETCH_INIT, fetcher.getState());

    try {
      fetcher.get();
      if (type.equals(FetchType.LOCAL)) {
        fail();
      }
    } catch (IllegalArgumentException e) {
      if (!type.equals(FetchType.LOCAL)) {
        fail();
      }
    }
    assertEquals(TajoProtos.FetcherState.FETCH_FAILED, fetcher.getState());
  }

  @Test
  public void testServerFailure() throws Exception {
    QueryId queryId = QueryIdFactory.NULL_QUERY_ID;
    String sid = "1";
    String ta = "1_0";
    String partId = "1";

    PullServerRequestURIBuilder builder = new PullServerRequestURIBuilder("127.0.0.1", pullServerService.getPort(),
        maxUrlLength);
    builder.setRequestType(PullServerConstants.CHUNK_REQUEST_PARAM_STRING)
        .setQueryId(queryId.toString())
        .setEbId(sid)
        .setPartId(partId)
        .setShuffleType(PullServerConstants.HASH_SHUFFLE_PARAM_STRING)
        .setTaskAttemptIds(Lists.newArrayList(ta));

    URI uri = builder.build(true).get(0);
    File data = new File(OUTPUT_DIR + "data");
    final AbstractFetcher fetcher;
    if (type.equals(FetchType.LOCAL)) {
      fetcher = new LocalFetcher(conf, uri, "test");
    } else {
      FileChunk storeChunk = new FileChunk(data, 0, data.length());
      storeChunk.setFromRemote(true);
      fetcher = new RemoteFetcher(conf, uri, storeChunk);
    }
    assertEquals(TajoProtos.FetcherState.FETCH_INIT, fetcher.getState());

    pullServerService.stop();

    boolean failure = false;
    try{
      fetcher.get();
    } catch (IOException e){
      failure = true;
    }
    assertTrue(failure);
    assertEquals(TajoProtos.FetcherState.FETCH_FAILED, fetcher.getState());
  }
}
