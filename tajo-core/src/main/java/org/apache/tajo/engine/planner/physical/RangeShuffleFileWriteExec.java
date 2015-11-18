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

package org.apache.tajo.engine.planner.physical;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.AnalyzedSortSpec;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.FreqHistogram;
import org.apache.tajo.catalog.statistics.HistogramUtil;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.KeyProjector;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.plan.logical.ShuffleFileWriteNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * <code>RangeShuffleFileWriteExec</code> is a physical executor to store intermediate data into a number of
 * file outputs associated with shuffle key ranges. The file outputs are stored with index files on local disks.
 * <code>RangeShuffleFileWriteExec</code> is implemented with an assumption that input tuples are sorted in an
 * specified order of shuffle keys.
 */
public class RangeShuffleFileWriteExec extends UnaryPhysicalExec {
  private final static Log LOG = LogFactory.getLog(RangeShuffleFileWriteExec.class);
  private final SortSpec[] sortSpecs;
  private Schema keySchema;

  private BSTIndex.BSTIndexWriter indexWriter;
  private TupleComparator comp;
  private FileAppender appender;
  private TableMeta meta;

  private KeyProjector keyProjector;

//  private FreqHistogram freqHistogram;

  private List<Pair<Tuple, Long>> countPerTuples;

  public RangeShuffleFileWriteExec(final TaskAttemptContext context,
                                   final ShuffleFileWriteNode plan,
                                   final PhysicalExec child, final SortSpec[] sortSpecs) throws IOException {
    super(context, plan.getInSchema(), plan.getInSchema(), child);
    this.sortSpecs = sortSpecs;

    if (plan.hasOptions()) {
      this.meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      this.meta = CatalogUtil.newTableMeta(plan.getStorageType());
    }
  }

  public void init() throws IOException {

    keySchema = PlannerUtil.sortSpecsToSchema(sortSpecs);
    keyProjector = new KeyProjector(inSchema, keySchema.toArray());

    BSTIndex bst = new BSTIndex(new TajoConf());
    this.comp = new BaseTupleComparator(keySchema, sortSpecs);
    Path storeTablePath = new Path(context.getWorkDir(), "output");
    LOG.info("Output data directory: " + storeTablePath);

    FileSystem fs = new RawLocalFileSystem();
    fs.mkdirs(storeTablePath);
    this.appender = (FileAppender) ((FileTablespace) TablespaceManager.getDefault())
        .getAppender(meta, outSchema, new Path(storeTablePath, "output"));
    this.appender.enableStats(keySchema.getAllColumns());
    this.appender.init();
    this.indexWriter = bst.getIndexWriter(new Path(storeTablePath, "index"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    this.indexWriter.setLoadNum(100);
    this.indexWriter.open();

//    this.freqHistogram = new FreqHistogram(keySchema, sortSpecs);
    countPerTuples = new ArrayList<>();

    super.init();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    Tuple keyTuple = null;
    Tuple prevKeyTuple = new VTuple(keySchema.size());
    long offset;
    long count = 0;

    while(!context.isStopped() && (tuple = child.next()) != null) {
      offset = appender.getOffset();
      appender.addTuple(tuple);
      keyTuple = keyProjector.project(tuple);
      if (!prevKeyTuple.equals(keyTuple)) {
        if (!prevKeyTuple.isBlank(0)) {
          // [prevKeyTuple, keyTuple)
//          freqHistogram.updateBucket(prevKeyTuple, keyTuple, HistogramUtil.increment(sortSpecs, null, keyTuple, prevKeyTuple, -1), count);
          countPerTuples.add(new Pair<>(new VTuple(prevKeyTuple.getValues()), count));
        }
        indexWriter.write(keyTuple, offset);
        prevKeyTuple.put(keyTuple.getValues());
        count = 0;
      }
      count++;
    }

    if (count > 0) {
//      Tuple infiniteTuple = HistogramUtil.getPositiveInfiniteTuple(keySchema.size());
//      Tuple interval = freqHistogram.getNonZeroMinInterval();
//      freqHistogram.updateBucket(prevKeyTuple, HistogramUtil.increment(sortSpecs, null, prevKeyTuple, interval, 1), interval, count);
      countPerTuples.add(new Pair<>(prevKeyTuple, count));
    }

    return null;
  }

  @Override
  public void rescan() throws IOException {
  }

  public void close() throws IOException {
    super.close();

    appender.flush();
    IOUtils.cleanup(LOG, appender);
    indexWriter.flush();
    IOUtils.cleanup(LOG, indexWriter);

    // Collect statistics data
    context.setResultStats(appender.getStats());
    context.addShuffleFileOutput(0, context.getTaskId().toString());

    // Check isPureAscii and find the maxLength
    List<ColumnStats> sortKeyStats = TupleUtil.extractSortColumnStats(sortSpecs, context.getResultStats().getColumnStats(), false);
    AnalyzedSortSpec[] analyzedSpecs = HistogramUtil.toAnalyzedSortSpecs(sortSpecs, sortKeyStats);

    for (Pair<Tuple, Long> eachPair : countPerTuples) {
      Tuple tuple = eachPair.getFirst();
      for (int i = 0; i < keySchema.size(); i++) {
        if (keySchema.getColumn(i).getDataType().getType().equals(Type.TEXT)) {
          boolean isCurrentPureAscii = StringUtils.isPureAscii(tuple.getText(i));
          if (analyzedSpecs[i].isPureAscii()) {
            analyzedSpecs[i].setPureAscii(isCurrentPureAscii);
          }
          if (isCurrentPureAscii) {
            analyzedSpecs[i].setMaxLength(Math.max(analyzedSpecs[i].getMaxLength(), tuple.getText(i).length()));
          } else {
            analyzedSpecs[i].setMaxLength(Math.max(analyzedSpecs[i].getMaxLength(), tuple.getUnicodeChars(i).length));
          }
        }
      }
    }

    FreqHistogram freqHistogram = new FreqHistogram(sortSpecs);
    Pair<Tuple, Long> current, next = null;
    for (int i = 0; i < countPerTuples.size() - 1; i++) {
      current = countPerTuples.get(i);
      next = countPerTuples.get(i + 1);

      freqHistogram.updateBucket(new TupleRange(current.getFirst(), next.getFirst(),
          freqHistogram.getComparator()), current.getSecond());
    }
    current = countPerTuples.get(countPerTuples.size() - 1);
    freqHistogram.updateBucket(new TupleRange(current.getFirst(), current.getFirst(),
        freqHistogram.getComparator()), current.getSecond(), true);
    freqHistogram.getBucket(current.getFirst(), current.getFirst()).setEndKeyInclusive(true);
    context.setFreqHistogram(freqHistogram);
    appender = null;
    indexWriter = null;
  }
}
