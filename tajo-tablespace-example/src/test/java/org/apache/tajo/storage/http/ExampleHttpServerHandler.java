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

package org.apache.tajo.storage.http;

import com.google.common.base.Preconditions;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaders.Names;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.util.Pair;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class ExampleHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

  private static final Log LOG = LogFactory.getLog(ExampleHttpServerHandler.class);

  public static final String BYTE_RANGE_PREFIX = "bytes=";

  private final boolean rangeRequestEnabled;

  public ExampleHttpServerHandler(boolean rangeRequestEnabled) throws IOException {
    this.rangeRequestEnabled = rangeRequestEnabled;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {

    if (request.getMethod().equals(HttpMethod.HEAD)) {

      processHead(context, request);

    } else if (request.getMethod().equals(HttpMethod.GET)) {

      processGet(context, request);

    } else {
      // error
      LOG.error("Not supported method: " + request.getMethod());
      context.writeAndFlush(getBadRequest());
    }
  }

  private void processHead(ChannelHandlerContext context, FullHttpRequest request) {
    HttpHeaders headers = request.headers();
    FullHttpResponse response = null;

    if (headers.contains(Names.RANGE)) {

      response = new DefaultFullHttpResponse(
          HTTP_1_1,
          request.getDecoderResult().isSuccess() ?
              rangeRequestEnabled ? PARTIAL_CONTENT : OK : BAD_REQUEST
      );

    }

    if (headers.contains(Names.CONTENT_LENGTH)) {

      try {
        File file = getRequestedFile(request.getUri());

        if (response == null) {

          response = new DefaultFullHttpResponse(
              HTTP_1_1,
              request.getDecoderResult().isSuccess() ? OK : BAD_REQUEST
          );

        }

        if (file.getName().equals("nation.tbl")) {
          HttpHeaders.setContentLength(response, 300 * 1024 * 1024);
        } else {
          HttpHeaders.setContentLength(response, file.length());

        }


      } catch (FileNotFoundException | URISyntaxException e) {
        response = getBadRequest();
      }
    }
    context.writeAndFlush(response);
  }

  private void processGet(ChannelHandlerContext context, FullHttpRequest request) {
    try {
      File file = getRequestedFile(request.getUri());

      String rangeParam = HttpHeaders.getHeader(request, Names.RANGE);

      RandomAccessFile raf = new RandomAccessFile(file, "r");
      long fileLength = raf.length();

      Pair<Long, Long> offsets = getOffsets(rangeParam, fileLength);
      raf.seek(offsets.getFirst());
      long contentLength = offsets.getSecond() - offsets.getFirst() + 1;

      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      HttpHeaders.setContentLength(response, contentLength);
      setContentTypeHeader(response, file);

      context.write(response);

      context.write(new DefaultFileRegion(raf.getChannel(), offsets.getFirst(), offsets.getFirst() + contentLength));

      // Write the end marker.
      ChannelFuture future = context.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
      future.addListener(ChannelFutureListener.CLOSE);

    } catch (IOException | URISyntaxException e) {
      context.writeAndFlush(getBadRequest());
    }
  }

  private static Pair<Long, Long> getOffsets(String rangeParam, long fileLength) {
    String prefix = rangeParam.substring(0, 6);
    Preconditions.checkArgument(prefix.equals(BYTE_RANGE_PREFIX));

    String[] rangeStrings = rangeParam.substring(6).split("-");
    // Multiple range params are not allowed in this test.
    Preconditions.checkArgument(rangeStrings.length > 0 && rangeStrings.length < 3);

    if (rangeStrings.length == 1) {
      return new Pair<>(Long.parseLong(rangeStrings[0]), fileLength - 1);
    } else {
      return new Pair<>(Long.parseLong(rangeStrings[0]), Long.parseLong(rangeStrings[1]));
    }
  }

  private static File getRequestedFile(String uri) throws FileNotFoundException, URISyntaxException {
    String path = URI.create(uri).getPath();
    URL url = ClassLoader.getSystemResource("tpch/" + path);
    if (url == null) {
      throw new FileNotFoundException(uri);
    }
    return new File(url.toURI());
  }

  private static FullHttpResponse getBadRequest() {
    // TODO: set cause
    return new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    LOG.error(cause.getMessage(), cause);
    if (context.channel().isOpen()) {
      context.channel().close();
    }
  }

  /**
   * Sets the content type header for the HTTP Response
   *
   * @param response
   *            HTTP response
   * @param file
   *            file to extract content type
   */
  private static void setContentTypeHeader(HttpResponse response, File file) {
    MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
    response.headers().set(CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));
  }
}
