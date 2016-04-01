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

package org.apache.tajo.pullserver;

import com.google.common.base.Preconditions;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.Pair;

import java.io.FileDescriptor;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PullServerUtil {
  private static final Log LOG = LogFactory.getLog(PullServerUtil.class);

  private static boolean nativeIOPossible = false;
  private static Method posixFadviseIfPossible;

  static {
    if (NativeIO.isAvailable() && loadNativeIO()) {
      nativeIOPossible = true;
    } else {
      LOG.warn("Unable to load hadoop nativeIO");
    }
  }

  public static boolean isNativeIOPossible() {
    return nativeIOPossible;
  }

  /**
   * Call posix_fadvise on the given file descriptor. See the manpage
   * for this syscall for more information. On systems where this
   * call is not available, does nothing.
   */
  public static void posixFadviseIfPossible(String identifier, java.io.FileDescriptor fd,
                                            long offset, long len, int flags) {
    if (nativeIOPossible) {
      try {
        posixFadviseIfPossible.invoke(null, identifier, fd, offset, len, flags);
      } catch (Throwable t) {
        nativeIOPossible = false;
        LOG.warn("Failed to manage OS cache for " + identifier, t);
      }
    }
  }

  /* load hadoop native method if possible */
  private static boolean loadNativeIO() {
    boolean loaded = true;
    if (nativeIOPossible) return loaded;

    Class[] parameters = {String.class, FileDescriptor.class, Long.TYPE, Long.TYPE, Integer.TYPE};
    try {
      Method getCacheManipulator = MethodUtils.getAccessibleMethod(NativeIO.POSIX.class, "getCacheManipulator", new Class[0]);
      Class posixClass;
      if (getCacheManipulator != null) {
        Object posix = MethodUtils.invokeStaticMethod(NativeIO.POSIX.class, "getCacheManipulator", null);
        posixClass = posix.getClass();
      } else {
        posixClass = NativeIO.POSIX.class;
      }
      posixFadviseIfPossible = MethodUtils.getAccessibleMethod(posixClass, "posixFadviseIfPossible", parameters);
    } catch (Throwable e) {
      loaded = false;
      LOG.warn("Failed to access posixFadviseIfPossible :" + e.getMessage(), e);
    }

    if (posixFadviseIfPossible == null) {
      loaded = false;
    }
    return loaded;
  }

  public static Path getBaseOutputDir(String queryId, String executionBlockSequenceId) {
    return StorageUtil.concatPath(
            queryId,
            "output",
            executionBlockSequenceId);
  }

  public static Path getBaseInputDir(String queryId, String executionBlockId) {
    return StorageUtil.concatPath(
            queryId,
            "in",
            executionBlockId);
  }

  public static List<String> splitMaps(List<String> mapq) {
    if (null == mapq) {
      return null;
    }
    final List<String> ret = new ArrayList<>();
    for (String s : mapq) {
      Collections.addAll(ret, s.split(","));
    }
    return ret;
  }

  public enum Param {
    // Common params
    REQUEST_TYPE("rtype"),  // can be one of 'm' for meta and 'c' for chunk.
    SHUFFLE_TYPE("stype"),  // can be one of 'r', 'h', and 's'.
    QUERY_ID("qid"),
    EB_ID("sid"),
    PART_ID("p"),
    TASK_ID("ta"),
    OFFSET("offset"),
    LENGTH("length"),

    // Range shuffle params
    START("start"),
    END("end"),
    FINAL("final");

    private String key;

    Param(String key) {
      this.key = key;
    }

    public String key() {
      return key;
    }
  }

  public static final String CHUNK_REQUEST_PARAM_STRING = "c";
  public static final String META_REQUEST_PARAM_STRING = "m";

  public static final String RANGE_SHUFFLE_PARAM_STRING = "r";
  public static final String HASH_SHUFFLE_PARAM_STRING = "h";
  public static final String SCATTERED_HASH_SHUFFLE_PARAM_STRING = "s";

  public static boolean isChunkRequest(String requestType) {
    return requestType.equals(CHUNK_REQUEST_PARAM_STRING);
  }

  public static boolean isMetaRequest(String requestType) {
    return requestType.equals(META_REQUEST_PARAM_STRING);
  }

  public static boolean isRangeShuffle(String shuffleType) {
    return shuffleType.equals(RANGE_SHUFFLE_PARAM_STRING);
  }

  public static boolean isHashShuffle(String shuffleType) {
    return shuffleType.equals(HASH_SHUFFLE_PARAM_STRING)
        || shuffleType.equals(SCATTERED_HASH_SHUFFLE_PARAM_STRING);
  }

  public static class PullServerParams extends HashMap<String, List<String>> {

    public PullServerParams(String uri) {
      super(new QueryStringDecoder(uri).parameters());
    }

    public boolean contains(Param param) {
      return containsKey(param.key());
    }

    public List<String> get(Param param) {
      return get(param.key());
    }

    private String checkAndGetFirstParam(Param param) {
      Preconditions.checkArgument(contains(param), "Missing " + param.name());
      Preconditions.checkArgument(get(param).size() == 1, "Too many params: " + param.name());
      return get(param).get(0);
    }

    private List<String> checkAndGet(Param param) {
      Preconditions.checkArgument(contains(param), "Missing " + param.name());
      return get(param);
    }

    public String requestType() {
      return checkAndGetFirstParam(Param.REQUEST_TYPE);
    }

    public String shuffleType() {
      return checkAndGetFirstParam(Param.SHUFFLE_TYPE);
    }

    public String queryId() {
      return checkAndGetFirstParam(Param.QUERY_ID);
    }

    public String ebId() {
      return checkAndGetFirstParam(Param.EB_ID);
    }

    public long offset() {
      return contains(Param.OFFSET) && get(Param.OFFSET).size() == 1 ?
          Long.parseLong(get(Param.OFFSET).get(0)) : -1L;
    }

    public long length() {
      return contains(Param.LENGTH) && get(Param.LENGTH).size() == 1 ?
          Long.parseLong(get(Param.LENGTH).get(0)) : -1L;
    }

    public String startKey() {
      return checkAndGetFirstParam(Param.START);
    }

    public String endKey() {
      return checkAndGetFirstParam(Param.END);
    }

    public boolean last() {
      return contains(Param.FINAL);
    }

    public String partId() {
      return checkAndGetFirstParam(Param.PART_ID);
    }

    public List<String> taskAttemptIds() {
      return checkAndGet(Param.TASK_ID);
    }
  }

  public static class PullServerRequestURIBuilder {
    private final StringBuilder builder = new StringBuilder("http://");
    private String requestType;
    private String shuffleType;
    private String queryId;
    private Integer ebId;
    private Integer partId;
    private List<Integer> taskIds;
    private List<Integer> attemptIds;
    private List<String> taskAttemptIds;
    private Long offset;
    private Long length;
    private String startKey;
    private String endKey;
    private boolean last;
    private final int maxUrlLength;

    public PullServerRequestURIBuilder(String pullServerAddr, int pullServerPort, int maxUrlLength) {
      builder.append(NetUtils.createSocketAddr(pullServerAddr, pullServerPort)).append("/?");
      this.maxUrlLength = maxUrlLength;
    }

    public PullServerRequestURIBuilder(String pullServerAddr, String pullServerPort, int maxUrlLength) {
      builder.append(pullServerAddr).append(":").append(pullServerPort).append("/?");
      this.maxUrlLength = maxUrlLength;
    }

    public List<URI> build(boolean includeTasks) {
      append(Param.REQUEST_TYPE, requestType)
          .append(Param.QUERY_ID, queryId)
          .append(Param.EB_ID, ebId)
          .append(Param.PART_ID, partId)
          .append(Param.SHUFFLE_TYPE, shuffleType);

      if (startKey != null) {
        String firstKeyBase64 = new String(Base64.encodeBase64(startKey.getBytes()));
        String endKeyBase64 = new String(Base64.encodeBase64(endKey.getBytes()));

        try {
          append(Param.START, URLEncoder.encode(firstKeyBase64, "utf-8"))
              .append(Param.END, URLEncoder.encode(endKeyBase64, "utf-8"));
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }

        if (last) {
          append(Param.FINAL, Boolean.toString(last));
        }
      }

      if (length != null) {
        append(Param.OFFSET, offset.toString())
            .append(Param.LENGTH, length.toString());
      }

      List<URI> results = new ArrayList<>();
      if (!includeTasks || isHashShuffle(shuffleType)) {
        results.add(URI.create(builder.toString()));
      } else {
        List<String> taskAttemptIds = this.taskAttemptIds;
        if (taskAttemptIds == null) {

          // Sort task ids to increase cache hit in pull server
          taskAttemptIds = IntStream.range(0, taskIds.size())
              .mapToObj(i -> new Pair<>(taskIds.get(i), attemptIds.get(i)))
              .sorted((p1, p2) -> p1.getFirst() - p2.getFirst())
              // In the case of hash shuffle each partition has single shuffle file per worker.
              // TODO If file is large, consider multiple fetching(shuffle file can be split)
              .filter(pair -> pair.getFirst() >= 0)
              .map(pair -> pair.getFirst() + "_" + pair.getSecond())
              .collect(Collectors.toList());
        }

        // If the get request is longer than 2000 characters,
        // the long request uri may cause HTTP Status Code - 414 Request-URI Too Long.
        // Refer to http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.15
        // The below code transforms a long request to multiple requests.
        List<String> taskIdsParams = new ArrayList<>();
        StringBuilder taskIdListBuilder = new StringBuilder();

        boolean first = true;
        for (int i = 0; i < taskAttemptIds.size(); i++) {
          if (!first) {
            taskIdListBuilder.append(",");
          }
          first = false;

          if (builder.length() + taskIdListBuilder.length() > maxUrlLength) {
            taskIdsParams.add(taskIdListBuilder.toString());
            taskIdListBuilder = new StringBuilder(taskAttemptIds.get(i));
          } else {
            taskIdListBuilder.append(taskAttemptIds.get(i));
          }
        }
        // if the url params remain
        if (taskIdListBuilder.length() > 0) {
          taskIdsParams.add(taskIdListBuilder.toString());
        }
        for (String param : taskIdsParams) {
          results.add(URI.create(builder + param));
        }
      }

      return results;
    }

    private PullServerRequestURIBuilder append(Param key, Object val) {
      builder.append(key.key)
          .append("=")
          .append(val)
          .append("&");

      return this;
    }

    public PullServerRequestURIBuilder setRequestType(String type) {
      this.requestType = type;
      return this;
    }

    public PullServerRequestURIBuilder setShuffleType(String shuffleType) {
      this.shuffleType = shuffleType;
      return this;
    }

    public PullServerRequestURIBuilder setQueryId(String queryId) {
      this.queryId = queryId;
      return this;
    }

    public PullServerRequestURIBuilder setEbId(String ebId) {
      this.ebId = Integer.parseInt(ebId);
      return this;
    }

    public PullServerRequestURIBuilder setEbId(Integer ebId) {
      this.ebId = ebId;
      return this;
    }

    public PullServerRequestURIBuilder setPartId(String partId) {
      this.partId = Integer.parseInt(partId);
      return this;
    }

    public PullServerRequestURIBuilder setPartId(Integer partId) {
      this.partId = partId;
      return this;
    }

    public PullServerRequestURIBuilder setTaskIds(List<Integer> taskIds) {
      this.taskIds = taskIds;
      return this;
    }

    public PullServerRequestURIBuilder setAttemptIds(List<Integer> attemptIds) {
      this.attemptIds = attemptIds;
      return this;
    }

    public PullServerRequestURIBuilder setTaskAttemptIds(List<String> taskAttemptIds) {
      this.taskAttemptIds = taskAttemptIds;
      return this;
    }

    public PullServerRequestURIBuilder setOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public PullServerRequestURIBuilder setLength(long length) {
      this.length = length;
      return this;
    }

    public PullServerRequestURIBuilder setStartKey(String startKey) {
      this.startKey = startKey;
      return this;
    }

    public PullServerRequestURIBuilder setEndKey(String endKey) {
      this.endKey = endKey;
      return this;
    }

    public PullServerRequestURIBuilder setLast(boolean last) {
      this.last = last;
      return this;
    }
  }
}
