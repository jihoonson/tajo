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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExampleHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Log LOG = LogFactory.getLog(ExampleHttpServerHandler.class);

  // head content length
  // head range
  // get file

  @Override
  protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest request) throws Exception {

    if (request.getMethod().equals(HttpMethod.HEAD)) {

    } else if (request.getMethod().equals(HttpMethod.GET)) {

    } else {
      // error
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    LOG.error(cause.getMessage(), cause);
    if (context.channel().isOpen()) {
      context.channel().close();
    }
  }
}
