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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaders.Names;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;

public class ExampleHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Log LOG = LogFactory.getLog(ExampleHttpServerHandler.class);

  private final boolean rangeRequestEnabled;

  // tpch tables

  public ExampleHttpServerHandler(boolean rangeRequestEnabled) {
    this.rangeRequestEnabled = rangeRequestEnabled;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {

    if (request.getMethod().equals(HttpMethod.HEAD)) {
      HttpHeaders headers = request.headers();

      FullHttpResponse response = null;
      if (headers.contains(Names.RANGE)) {
        response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            request.getDecoderResult().isSuccess() ?
                rangeRequestEnabled ? HttpResponseStatus.PARTIAL_CONTENT :
                    HttpResponseStatus.OK : HttpResponseStatus.BAD_REQUEST
        );
      }

      if (headers.contains(Names.CONTENT_LENGTH)) {
        if (response == null) {
          response = new DefaultFullHttpResponse(
              HttpVersion.HTTP_1_1,
              request.getDecoderResult().isSuccess() ? HttpResponseStatus.OK : HttpResponseStatus.BAD_REQUEST
          );
        }
        HttpHeaders.setContentLength(response, 30000);
      }
      context.writeAndFlush(response);

    } else if (request.getMethod().equals(HttpMethod.GET)) {
      FullHttpResponse response = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1,
          HttpResponseStatus.BAD_REQUEST
      );
      context.writeAndFlush(response);

    } else {
      // error
      LOG.error("Not supported method: " + request.getMethod());

      FullHttpResponse response = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1,
          HttpResponseStatus.BAD_REQUEST
      );
      context.writeAndFlush(response);

    }
  }

  private ChannelFuture sendFile(ChannelHandlerContext ctx,
                                 File file, long startOFfset, long length) throws IOException {
    return null;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    LOG.error(cause.getMessage(), cause);
    if (context.channel().isOpen()) {
      context.channel().close();
    }
  }
}
