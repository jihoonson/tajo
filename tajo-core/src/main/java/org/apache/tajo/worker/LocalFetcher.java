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

import com.google.gson.Gson;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.TajoProtos.FetcherState;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.pullserver.PullServerUtil;
import org.apache.tajo.pullserver.PullServerUtil.Param;
import org.apache.tajo.pullserver.PullServerUtil.PullServerParams;
import org.apache.tajo.pullserver.PullServerUtil.PullServerRequestURIBuilder;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.pullserver.retriever.FileChunk;
import org.apache.tajo.pullserver.retriever.FileChunkMeta;
import org.apache.tajo.rpc.NettyUtils;
import org.apache.tajo.storage.HashShuffleAppenderManager;
import org.apache.tajo.storage.StorageUtil;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class LocalFetcher extends AbstractFetcher {

  private final static Log LOG = LogFactory.getLog(LocalFetcher.class);

  private final ExecutionBlockContext executionBlockContext;
  private final TajoPullServerService pullServerService;

  private final String host;
  private int port;
  private final Bootstrap bootstrap;
  private final int maxUrlLength;
  private final List<FileChunkMeta> chunkMetas = new ArrayList<>();
  private final String tableName;

  public LocalFetcher(TajoConf conf, URI uri, ExecutionBlockContext executionBlockContext, String tableName) {
    super(conf, uri);
    this.executionBlockContext = executionBlockContext;
    this.maxUrlLength = conf.getIntVar(ConfVars.PULLSERVER_FETCH_URL_MAX_LENGTH);
    this.tableName = tableName;

    Optional<TajoPullServerService> optional = executionBlockContext.getSharedResource().getPullServerService();
    if (optional.isPresent()) {
      // local pull server service
      this.pullServerService = optional.get();
      this.host = null;
      this.bootstrap = null;
    } else if (PullServerUtil.useExternalPullServerService(conf)) {
      // external pull server service
      pullServerService = null;

      String scheme = uri.getScheme() == null ? "http" : uri.getScheme();
      this.host = uri.getHost() == null ? "localhost" : uri.getHost();
      this.port = uri.getPort();
      if (port == -1) {
        if (scheme.equalsIgnoreCase("http")) {
          this.port = 80;
        } else if (scheme.equalsIgnoreCase("https")) {
          this.port = 443;
        }
      }

      bootstrap = new Bootstrap()
          .group(
              NettyUtils.getSharedEventLoopGroup(NettyUtils.GROUP.FETCHER,
                  conf.getIntVar(TajoConf.ConfVars.SHUFFLE_RPC_CLIENT_WORKER_THREAD_NUM)))
          .channel(NioSocketChannel.class)
          .option(ChannelOption.ALLOCATOR, NettyUtils.ALLOCATOR)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
              conf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_CONNECT_TIMEOUT) * 1000)
          .option(ChannelOption.SO_RCVBUF, 1048576) // set 1M
          .option(ChannelOption.TCP_NODELAY, true);
    } else {
      throw new TajoInternalError("Pull server service is not specified");
    }
  }

  @Override
  public List<FileChunk> get() throws IOException {
    return pullServerService != null ? getFromInternalPullServer() : getFromExternalPullServer();
  }

  private List<FileChunk> getFromInternalPullServer() {
    List<FileChunk> fileChunks = new ArrayList<>();
    startTime = System.currentTimeMillis();
    finishTime = System.currentTimeMillis();
    state = TajoProtos.FetcherState.FETCH_FINISHED;
    fileChunks.add(fileChunk);
    fileLen = fileChunk.getFile().length();
    return fileChunks;
  }

  private List<FileChunk> getFromExternalPullServer() throws IOException {
    final PullServerParams params = new PullServerParams(uri.toString());
    Path queryBaseDir = PullServerUtil.getBaseOutputDir(params.queryId(), params.ebId());
    List<FileChunk> fileChunks = new ArrayList<>();

    if (params.shuffleType().equals(PullServerUtil.RANGE_SHUFFLE_PARAM_STRING)) {
      if (state == FetcherState.FETCH_INIT) {
        ChannelInitializer<Channel> initializer = new HttpClientChannelInitializer();
        bootstrap.handler(initializer);
      }

      this.startTime = System.currentTimeMillis();
      this.state = TajoProtos.FetcherState.FETCH_FETCHING;
      ChannelFuture future = null;
      try {
        future = bootstrap.clone().connect(new InetSocketAddress(host, port))
            .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);

        // Wait until the connection attempt succeeds or fails.
        Channel channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess()) {
          state = TajoProtos.FetcherState.FETCH_FAILED;
          throw new IOException(future.cause());
        }

        List<URI> metaRequestURIs = createChunkMetaRequestURL(host, port, params);

        for (URI eachURI : metaRequestURIs) {
          String query = eachURI.getPath()
              + (eachURI.getRawQuery() != null ? "?" + eachURI.getRawQuery() : "");
          LOG.info("meta request query: " + query);
          HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, query);
          request.headers().set(HttpHeaders.Names.HOST, host);
          request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
          request.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);

          if(LOG.isDebugEnabled()) {
            LOG.debug("Status: " + getState() + ", URI:" + eachURI);
          }
          // Send the HTTP request.
          channel.writeAndFlush(request);
        }
        // Wait for the server to close the connection. throw exception if failed
        channel.closeFuture().syncUninterruptibly();

        for (FileChunkMeta eachMeta : chunkMetas) {
          Path outputPath = StorageUtil.concatPath(queryBaseDir, eachMeta.getTaskId(), "output");
          if (!executionBlockContext.getLocalDirAllocator().ifExists(outputPath.toString(), conf)) {
            LOG.warn("Range shuffle - file not exist. " + outputPath);
            continue;
          }
          Path path = executionBlockContext.getLocalFS().makeQualified(
              executionBlockContext.getLocalDirAllocator().getLocalPathToRead(outputPath.toString(), conf));
          FileChunk chunk = new FileChunk(new File(URI.create(path.toUri() + "/output")),
              eachMeta.getStartOffset(), eachMeta.getLength());
          chunk.setEbId(tableName);
          fileChunks.add(chunk);
        }

        LOG.info("# of file chunks: " + fileChunks.size());
        return fileChunks;
      } finally {
        if(future != null && future.channel().isOpen()){
          // Close the channel to exit.
          future.channel().close().awaitUninterruptibly();
        }

        this.finishTime = System.currentTimeMillis();
      }
    } else {
      String partId = params.partId();
      long offset = params.offset();
      long length = params.length();
      int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
      Path partPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);

      if (!executionBlockContext.getLocalDirAllocator().ifExists(partPath.toString(), conf)) {
        throw new IOException("Hash shuffle or Scattered hash shuffle - file not exist: " + partPath);
      }
      Path path = executionBlockContext.getLocalFS().makeQualified(
          executionBlockContext.getLocalDirAllocator().getLocalPathToRead(partPath.toString(), conf));
      File file = new File(path.toUri());
      long startPos = (offset >= 0 && length >= 0) ? offset : 0;
      long readLen = (offset >= 0 && length >= 0) ? length : file.length();

      if (startPos >= file.length()) {
        throw new IOException("Start pos[" + startPos + "] great than file length [" + file.length() + "]");
      }
      FileChunk chunk = new FileChunk(file, startPos, readLen);
      chunk.setEbId(tableName);
      if (chunk.length() > 0) {
        fileChunks.add(chunk);
      }
      state = FetcherState.FETCH_FINISHED;
      return fileChunks;
    }
  }

  public class HttpClientHandler extends ChannelInboundHandlerAdapter {
    private int length = -1;
    private int totalReceivedContentLength = 0;
    private byte[] buf;
    final Gson gson = new Gson();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
        throws Exception {

      messageReceiveCount++;
      if (msg instanceof HttpResponse) {
        try {
          HttpResponse response = (HttpResponse) msg;

          StringBuilder sb = new StringBuilder();
          if (LOG.isDebugEnabled()) {
            sb.append("STATUS: ").append(response.getStatus()).append(", VERSION: ")
                .append(response.getProtocolVersion()).append(", HEADER: ");
          }
          if (!response.headers().names().isEmpty()) {
            for (String name : response.headers().names()) {
              for (String value : response.headers().getAll(name)) {
                if (LOG.isDebugEnabled()) {
                  sb.append(name).append(" = ").append(value);
                }
                if (this.length == -1 && name.equals("Content-Length")) {
                  this.length = Integer.parseInt(value);
                  if (buf == null || buf.length < this.length) {
                    buf = new byte[this.length];
                  }
                }
              }
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(sb.toString());
          }

          if (response.getStatus().code() == HttpResponseStatus.NO_CONTENT.code()) {
            LOG.warn("There are no data corresponding to the request");
            length = 0;
            return;
          } else if (response.getStatus().code() != HttpResponseStatus.OK.code()) {
            LOG.error(response.getStatus().reasonPhrase());
            state = TajoProtos.FetcherState.FETCH_FAILED;
            return;
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        } finally {
          ReferenceCountUtil.release(msg);
        }
      }

      if (msg instanceof HttpContent) {
        HttpContent httpContent = (HttpContent) msg;
        ByteBuf content = httpContent.content();

        if (state != FetcherState.FETCH_FAILED) {
          try {
            if (content.isReadable()) {
              int contentLength = content.readableBytes();
              content.readBytes(buf, totalReceivedContentLength, contentLength);
              totalReceivedContentLength += contentLength;
              LOG.info("length: " + length + ", totalReceivedContentLength: " + totalReceivedContentLength);
              if (totalReceivedContentLength == length) {
                List<String> jsonMetas = gson.fromJson(new String(buf), List.class);
                for (String eachJson : jsonMetas) {
                  FileChunkMeta meta = gson.fromJson(eachJson, FileChunkMeta.class);
                  LOG.info("received file chunk meta: " + meta);
                  chunkMetas.add(meta);
                }
                totalReceivedContentLength = 0;
                length = -1;
              } else if (totalReceivedContentLength > length) {
                // TODO
                throw new IOException("Illegal length: " + totalReceivedContentLength + ", length: " + length);
              }
            }
            LOG.info("msg is instance of LastHttpContent: " + (msg instanceof LastHttpContent));
            if (msg instanceof LastHttpContent) {
              finishTime = System.currentTimeMillis();
              state = FetcherState.FETCH_FINISHED;
            }
            LOG.info("state: " + state);
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          } finally {
            ReferenceCountUtil.release(msg);
          }
        } else {
          // http content contains the reason why the fetch failed.
          LOG.error(content.toString(Charset.defaultCharset()));
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      if (cause instanceof ReadTimeoutException) {
        LOG.warn(cause.getMessage(), cause);
      } else {
        LOG.error("Fetch failed :", cause);
      }

      // this fetching will be retry
      finishTime = System.currentTimeMillis();
      state = TajoProtos.FetcherState.FETCH_FAILED;
      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      if(getState() != TajoProtos.FetcherState.FETCH_FINISHED){
        //channel is closed, but cannot complete fetcher
        finishTime = System.currentTimeMillis();
        LOG.error("Channel closed by peer: " + ctx.channel());
        state = TajoProtos.FetcherState.FETCH_FAILED;
      }

      super.channelUnregistered(ctx);
    }
  }

  public class HttpClientChannelInitializer extends ChannelInitializer<Channel> {

    @Override
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();

      int maxChunkSize = conf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_CHUNK_MAX_SIZE);
      int readTimeout = conf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_READ_TIMEOUT);

      pipeline.addLast("codec", new HttpClientCodec(4096, 8192, maxChunkSize));
      pipeline.addLast("inflater", new HttpContentDecompressor());
      pipeline.addLast("timeout", new ReadTimeoutHandler(readTimeout, TimeUnit.SECONDS));
      pipeline.addLast("handler", new HttpClientHandler());
    }
  }

  private List<URI> createChunkMetaRequestURL(String pullServerAddr, int pullServerPort, PullServerParams params) {
    PullServerRequestURIBuilder builder = new PullServerRequestURIBuilder(pullServerAddr, pullServerPort, maxUrlLength);
    builder.setRequestType(PullServerUtil.META_REQUEST_PARAM_STRING)
        .setQueryId(params.queryId())
        .setShuffleType(params.shuffleType())
        .setEbId(params.ebId())
        .setPartId(params.partId());

    if (params.contains(Param.OFFSET)) {
      builder.setOffset(params.offset()).setLength(params.length());
    }

    if (params.shuffleType().equals(PullServerUtil.RANGE_SHUFFLE_PARAM_STRING)) {
      builder.setStartKeyBase64(params.startKey())
          .setEndKeyBase64(params.endKey())
          .setLast(params.last());
    }

    if (params.contains(Param.TASK_ID)) {
      builder.setTaskAttemptIds(params.taskAttemptIds());
    }
    return builder.build(true);
  }

//  private List<FileChunk> getLocalStoredFileChunk(URI fetchURI, TajoConf conf) throws IOException {
//    // Parse the URI
//
//    // Parsing the URL into key-values
//    final Map<String, List<String>> params = PullServerUtil.decodeParams(fetchURI.toString());
//
//    String partId = params.get("p").get(0);
//    String queryId = params.get("qid").get(0);
//    String shuffleType = params.get("type").get(0);
//    String sid =  params.get("sid").get(0);
//
//    final List<String> taskIdList = params.get("ta");
//    final List<String> offsetList = params.get("offset");
//    final List<String> lengthList = params.get("length");
//
//    long offset = (offsetList != null && !offsetList.isEmpty()) ? Long.parseLong(offsetList.get(0)) : -1L;
//    long length = (lengthList != null && !lengthList.isEmpty()) ? Long.parseLong(lengthList.get(0)) : -1L;
//
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("PullServer request param: shuffleType=" + shuffleType + ", sid=" + sid + ", partId=" + partId
//          + ", taskIds=" + taskIdList);
//    }
//
//    // The working directory of Tajo worker for each query, including stage
//    Path queryBaseDir = PullServerUtil.getBaseOutputDir(queryId, sid);
//
//    List<FileChunk> chunkList = new ArrayList<>();
//    // If the stage requires a range shuffle
//    if (shuffleType.equals("r")) {
//
//      final String startKey = params.get("start").get(0);
//      final String endKey = params.get("end").get(0);
//      final boolean last = params.get("final") != null;
//      final List<String> taskIds = PullServerUtil.splitMaps(taskIdList);
//      final List<String> paths = new ArrayList<>();
//      for (String eachTaskId : taskIds) {
//        Path outputPath = StorageUtil.concatPath(queryBaseDir, eachTaskId, "output");
//        if (!executionBlockContext.getLocalDirAllocator().ifExists(outputPath.toString(), conf)) {
//          LOG.warn("Range shuffle - file not exist. " + outputPath);
//          continue;
//        }
//        Path path = executionBlockContext.getLocalFS().makeQualified(
//            executionBlockContext.getLocalDirAllocator().getLocalPathToRead(outputPath.toString(), conf));
//        paths.add(path.toString());
//      }
//
//      long before = System.currentTimeMillis();
//      try {
//        chunkList.addAll(TajoPullServerService.getFileChunks(queryId, sid, paths, startKey, endKey, last));
//
//      } catch (Throwable t) {
//        LOG.error(t.getMessage(), t);
//        throw new IOException(t.getCause());
//      }
//      long after = System.currentTimeMillis();
//      if (LOG.isDebugEnabled()) {
//        LOG.debug("Index lookup time: " + (after - before) + " ms");
//      }
//
//      // If the stage requires a hash shuffle or a scattered hash shuffle
//    } else if (shuffleType.equals("h") || shuffleType.equals("s")) {
//      int partParentId = HashShuffleAppenderManager.getPartParentId(Integer.parseInt(partId), conf);
//      Path partPath = StorageUtil.concatPath(queryBaseDir, "hash-shuffle", String.valueOf(partParentId), partId);
//
//      if (!executionBlockContext.getLocalDirAllocator().ifExists(partPath.toString(), conf)) {
//        throw new IOException("Hash shuffle or Scattered hash shuffle - file not exist: " + partPath);
//      }
//      Path path = executionBlockContext.getLocalFS().makeQualified(
//          executionBlockContext.getLocalDirAllocator().getLocalPathToRead(partPath.toString(), conf));
//      File file = new File(path.toUri());
//      long startPos = (offset >= 0 && length >= 0) ? offset : 0;
//      long readLen = (offset >= 0 && length >= 0) ? length : file.length();
//
//      if (startPos >= file.length()) {
//        throw new IOException("Start pos[" + startPos + "] great than file length [" + file.length() + "]");
//      }
//      FileChunk chunk = new FileChunk(file, startPos, readLen);
//      chunkList.add(chunk);
//
//    } else {
//      throw new IOException("Unknown shuffle type");
//    }
//
//    return chunkList;
//  }
}
