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

package org.apache.tajo.storage.orc2;

import com.facebook.hive.orc.PositionProviderImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderUtils;
import org.apache.hadoop.hive.ql.io.orc.SchemaEvolution;
import org.apache.hadoop.hive.ql.io.orc.TreeReaderFactory;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.*;
import org.apache.orc.impl.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.fragment.FileFragment;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrcRecordReader implements Closeable {

  private final Log LOG = LogFactory.getLog(OrcRecordReader.class);

  private final Path path;
  private final long firstRow;
  private final List<OrcProto.StripeInformation> stripes =
      new ArrayList<>();
  private OrcProto.StripeFooter stripeFooter;
  private final long totalRowCount;
  private final CompressionCodec codec;
  private final List<OrcProto.Type> types;
  private final int bufferSize;
  private final boolean[] included;
  private final long rowIndexStride;
  private long rowInStripe = 0;
  private int currentStripe = -1;
  private long rowBaseInStripe = 0;
  private long rowCountInStripe = 0;
  private final Map<StreamName, InStream> streams =
      new HashMap<StreamName, InStream>();
  DiskRangeList bufferChunks = null;
  private final TreeReaderFactory.TreeReader reader;
  private final OrcProto.RowIndex[] indexes;
  private final OrcProto.BloomFilterIndex[] bloomFilterIndices;
  // an array about which row groups aren't skipped
  private boolean[] includedRowGroups = null;
  private final Configuration conf;
  private final MetadataReader metadata;
  private final DataReader dataReader;

  /**
   * Given a list of column names, find the given column and return the index.
   *
   * @param columnNames the list of potential column names
   * @param columnName  the column name to look for
   * @param rootColumn  offset the result with the rootColumn
   * @return the column number or -1 if the column wasn't found
   */
  static int findColumns(String[] columnNames,
                         String columnName,
                         int rootColumn) {
    for(int i=0; i < columnNames.length; ++i) {
      if (columnName.equals(columnNames[i])) {
        return i + rootColumn;
      }
    }
    return -1;
  }

  protected OrcRecordReader(List<OrcProto.StripeInformation> stripes,
                            FileSystem fileSystem,
                            Schema schema,
                            Schema target,
                            FileFragment fragment,
                            boolean[] included,
                            boolean skipCorruptRecords,
                            List<OrcProto.Type> types,
                            CompressionCodec codec,
                            int bufferSize,
                            long strideRate,
                            Configuration conf
  ) throws IOException {

    TreeReaderFactory.TreeReaderSchema treeReaderSchema;

    // Now that we are creating a record reader for a file, validate that the schema to read
    // is compatible with the file schema.
    //
    List<OrcProto.Type> schemaTypes = org.apache.orc.OrcUtils.getOrcTypes(target);
    treeReaderSchema = SchemaEvolution.validateAndCreate(types, schemaTypes);

    this.path = fragment.getPath();
    this.codec = codec;
    this.types = types;
    this.bufferSize = bufferSize;
    this.included = included;
    this.conf = conf;
    this.rowIndexStride = strideRate;
    this.metadata = new MetadataReaderImpl(fileSystem, path, codec, bufferSize, types.size());

    long rows = 0;
    long skippedRows = 0;
    long offset = fragment.getStartKey();
    long maxOffset = fragment.getStartKey() + fragment.getLength();
    for(OrcProto.StripeInformation stripe: stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        skippedRows += stripe.getNumberOfRows();
      } else if (stripeStart < maxOffset) {
        this.stripes.add(stripe);
        rows += stripe.getNumberOfRows();
      }
    }

    // TODO: we could change the ctor to pass this externally
    this.dataReader = RecordReaderUtils.createDefaultDataReader(fileSystem, path, true, codec);
    this.dataReader.open();

    firstRow = skippedRows;
    totalRowCount = rows;
    Boolean skipCorrupt = skipCorruptRecords;

    reader = TreeReaderFactory.createTreeReader(0, treeReaderSchema, included, skipCorrupt);
    indexes = new OrcProto.RowIndex[types.size()];
    bloomFilterIndices = new OrcProto.BloomFilterIndex[types.size()];
    advanceToNextRow(reader, 0L, true);
  }

  /**
   * Plan the ranges of the file that we need to read given the list of
   * columns and row groups.
   *
   * @param streamList        the list of streams available
   * @param indexes           the indexes that have been loaded
   * @param includedColumns   which columns are needed
   * @param includedRowGroups which row groups are needed
   * @param isCompressed      does the file have generic compression
   * @param encodings         the encodings for each column
   * @param types             the types of the columns
   * @param compressionSize   the compression block size
   * @return the list of disk ranges that will be loaded
   */
  static DiskRangeList planReadPartialDataStreams
  (List<OrcProto.Stream> streamList,
   OrcProto.RowIndex[] indexes,
   boolean[] includedColumns,
   boolean[] includedRowGroups,
   boolean isCompressed,
   List<OrcProto.ColumnEncoding> encodings,
   List<OrcProto.Type> types,
   int compressionSize,
   boolean doMergeBuffers) {
    long offset = 0;
    // figure out which columns have a present stream
    boolean[] hasNull = RecordReaderUtils.findPresentStreamsByColumn(streamList, types);
    DiskRangeList.CreateHelper list = new DiskRangeList.CreateHelper();
    for (OrcProto.Stream stream : streamList) {
      long length = stream.getLength();
      int column = stream.getColumn();
      OrcProto.Stream.Kind streamKind = stream.getKind();
      // since stream kind is optional, first check if it exists
      if (stream.hasKind() &&
          (StreamName.getArea(streamKind) == StreamName.Area.DATA) &&
          includedColumns[column]) {
        // if we aren't filtering or it is a dictionary, load it.
        if (includedRowGroups == null
            || RecordReaderUtils.isDictionary(streamKind, encodings.get(column))) {
          RecordReaderUtils.addEntireStreamToRanges(offset, length, list, doMergeBuffers);
        } else {
          RecordReaderUtils.addRgFilteredStreamToRanges(stream, includedRowGroups,
              isCompressed, indexes[column], encodings.get(column), types.get(column),
              compressionSize, hasNull[column], offset, length, list, doMergeBuffers);
        }
      }
      offset += length;
    }
    return list.extract();
  }

  void createStreams(List<OrcProto.Stream> streamDescriptions,
                     DiskRangeList ranges,
                     boolean[] includeColumn,
                     CompressionCodec codec,
                     int bufferSize,
                     Map<StreamName, InStream> streams) throws IOException {
    long streamOffset = 0;
    for (OrcProto.Stream streamDesc : streamDescriptions) {
      int column = streamDesc.getColumn();
      if ((includeColumn != null && !includeColumn[column]) ||
          streamDesc.hasKind() &&
              (StreamName.getArea(streamDesc.getKind()) != StreamName.Area.DATA)) {
        streamOffset += streamDesc.getLength();
        continue;
      }
      List<DiskRange> buffers = RecordReaderUtils.getStreamBuffers(
          ranges, streamOffset, streamDesc.getLength());
      StreamName name = new StreamName(column, streamDesc.getKind());
      streams.put(name, InStream.create(name.toString(), buffers,
          streamDesc.getLength(), codec, bufferSize));
      streamOffset += streamDesc.getLength();
    }
  }

  private void readPartialDataStreams(StripeInformation stripe) throws IOException {
    List<OrcProto.Stream> streamList = stripeFooter.getStreamsList();
    DiskRangeList toRead = planReadPartialDataStreams(streamList,
        indexes, included, includedRowGroups, codec != null,
        stripeFooter.getColumnsList(), types, bufferSize, true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("chunks = " + RecordReaderUtils.stringifyDiskRanges(toRead));
    }
    bufferChunks = dataReader.readFileData(toRead, stripe.getOffset(), false);
    if (LOG.isDebugEnabled()) {
      LOG.debug("merge = " + RecordReaderUtils.stringifyDiskRanges(bufferChunks));
    }

    createStreams(streamList, bufferChunks, included, codec, bufferSize, streams);
  }

  /**
   * Skip over rows that we aren't selecting, so that the next row is
   * one that we will read.
   *
   * @param nextRow the row we want to go to
   * @throws IOException
   */
  private boolean advanceToNextRow(
      TreeReaderFactory.TreeReader reader, long nextRow, boolean canAdvanceStripe)
      throws IOException {
    long nextRowInStripe = nextRow - rowBaseInStripe;
    // check for row skipping
    if (rowIndexStride != 0 &&
        includedRowGroups != null &&
        nextRowInStripe < rowCountInStripe) {
      int rowGroup = (int) (nextRowInStripe / rowIndexStride);
      if (!includedRowGroups[rowGroup]) {
        while (rowGroup < includedRowGroups.length && !includedRowGroups[rowGroup]) {
          rowGroup += 1;
        }
        if (rowGroup >= includedRowGroups.length) {
          if (canAdvanceStripe) {
            advanceStripe();
          }
          return canAdvanceStripe;
        }
        nextRowInStripe = Math.min(rowCountInStripe, rowGroup * rowIndexStride);
      }
    }
    if (nextRowInStripe >= rowCountInStripe) {
      if (canAdvanceStripe) {
        advanceStripe();
      }
      return canAdvanceStripe;
    }
    if (nextRowInStripe != rowInStripe) {
      if (rowIndexStride != 0) {
        int rowGroup = (int) (nextRowInStripe / rowIndexStride);
        seekToRowEntry(reader, rowGroup);
        reader.skipRows(nextRowInStripe - rowGroup * rowIndexStride);
      } else {
        reader.skipRows(nextRowInStripe - rowInStripe);
      }
      rowInStripe = nextRowInStripe;
    }
    return true;
  }

  public boolean hasNext() throws IOException {
    return rowInStripe < rowCountInStripe;
  }

  public Object next(Object previous) throws IOException {
    try {
      final Object result = reader.next(previous);
      // find the next row
      rowInStripe += 1;
      advanceToNextRow(reader, rowInStripe + rowBaseInStripe, true);
      return result;
    } catch (IOException e) {
      // Rethrow exception with file name in log message
      throw new IOException("Error reading file: " + path, e);
    }
  }

  private long computeBatchSize(long targetBatchSize) {
    long batchSize = 0;
    // In case of PPD, batch size should be aware of row group boundaries. If only a subset of row
    // groups are selected then marker position is set to the end of range (subset of row groups
    // within strip). Batch size computed out of marker position makes sure that batch size is
    // aware of row group boundary and will not cause overflow when reading rows
    // illustration of this case is here https://issues.apache.org/jira/browse/HIVE-6287
    if (rowIndexStride != 0 && includedRowGroups != null && rowInStripe < rowCountInStripe) {
      int startRowGroup = (int) (rowInStripe / rowIndexStride);
      if (!includedRowGroups[startRowGroup]) {
        while (startRowGroup < includedRowGroups.length && !includedRowGroups[startRowGroup]) {
          startRowGroup += 1;
        }
      }

      int endRowGroup = startRowGroup;
      while (endRowGroup < includedRowGroups.length && includedRowGroups[endRowGroup]) {
        endRowGroup += 1;
      }

      final long markerPosition =
          (endRowGroup * rowIndexStride) < rowCountInStripe ? (endRowGroup * rowIndexStride)
              : rowCountInStripe;
      batchSize = Math.min(targetBatchSize, (markerPosition - rowInStripe));

      if (LOG.isDebugEnabled() && batchSize < targetBatchSize) {
        LOG.debug("markerPosition: " + markerPosition + " batchSize: " + batchSize);
      }
    } else {
      batchSize = Math.min(targetBatchSize, (rowCountInStripe - rowInStripe));
    }
    return batchSize;
  }

  /**
   * Read the next stripe until we find a row that we don't skip.
   *
   * @throws IOException
   */
  private void advanceStripe() throws IOException {
    rowInStripe = rowCountInStripe;
    while (rowInStripe >= rowCountInStripe &&
        currentStripe < stripes.size() - 1) {
      currentStripe += 1;
      readStripe();
    }
  }

  /**
   * Read the current stripe into memory.
   *
   * @throws IOException
   */
  private void readStripe() throws IOException {
    OrcProto.StripeInformation stripe = beginReadStripe();

    // if we haven't skipped the whole stripe, read the data
    if (rowInStripe < rowCountInStripe) {
      readAllDataStreams(stripe);
      reader.startStripe(streams, stripeFooter);
      // if we skipped the first row group, move the pointers forward
      if (rowInStripe != 0) {
        seekToRowEntry(reader, (int) (rowInStripe / rowIndexStride));
      }
    }
  }

  private void clearStreams() throws IOException {
    // explicit close of all streams to de-ref ByteBuffers
    for (InStream is : streams.values()) {
      is.close();
    }
    if (bufferChunks != null) {
      if (dataReader.isTrackingDiskRanges()) {
        for (DiskRangeList range = bufferChunks; range != null; range = range.next) {
          if (!(range instanceof BufferChunk)) {
            continue;
          }
          dataReader.releaseBuffer(((BufferChunk) range).getChunk());
        }
      }
    }
    bufferChunks = null;
    streams.clear();
  }

  OrcProto.StripeFooter readStripeFooter(OrcProto.StripeInformation stripe) throws IOException {
    return metadata.readStripeFooter(stripe);
  }

  private OrcProto.StripeInformation beginReadStripe() throws IOException {
    OrcProto.StripeInformation stripe = stripes.get(currentStripe);
    stripeFooter = readStripeFooter(stripe);
    clearStreams();
    // setup the position in the stripe
    rowCountInStripe = stripe.getNumberOfRows();
    rowInStripe = 0;
    rowBaseInStripe = 0;
    for (int i = 0; i < currentStripe; ++i) {
      rowBaseInStripe += stripes.get(i).getNumberOfRows();
    }
    // reset all of the indexes
    for (int i = 0; i < indexes.length; ++i) {
      indexes[i] = null;
    }
    return stripe;
  }

  private void readAllDataStreams(StripeInformation stripe) throws IOException {
    long start = stripe.getIndexLength();
    long end = start + stripe.getDataLength();
    // explicitly trigger 1 big read
    DiskRangeList toRead = new DiskRangeList(start, end);
    bufferChunks = dataReader.readFileData(toRead, stripe.getOffset(), false);
    List<OrcProto.Stream> streamDescriptions = stripeFooter.getStreamsList();
    createStreams(streamDescriptions, bufferChunks, null, codec, bufferSize, streams);
  }

  public long getRowNumber() {
    return rowInStripe + rowBaseInStripe + firstRow;
  }

  public float getProgress() {
    return ((float) rowBaseInStripe + rowInStripe) / totalRowCount;
  }

  MetadataReader getMetadataReader() {
    return metadata;
  }

  private int findStripe(long rowNumber) {
    for (int i = 0; i < stripes.size(); i++) {
      OrcProto.StripeInformation stripe = stripes.get(i);
      if (stripe.getNumberOfRows() > rowNumber) {
        return i;
      }
      rowNumber -= stripe.getNumberOfRows();
    }
    throw new IllegalArgumentException("Seek after the end of reader range");
  }

  OrcIndex readRowIndex(
      int stripeIndex, boolean[] included) throws IOException {
    return readRowIndex(stripeIndex, included, null, null);
  }

  OrcIndex readRowIndex(int stripeIndex, boolean[] included, OrcProto.RowIndex[] indexes,
                        OrcProto.BloomFilterIndex[] bloomFilterIndex) throws IOException {
    OrcProto.StripeInformation stripe = stripes.get(stripeIndex);
    OrcProto.StripeFooter stripeFooter = null;
    // if this is the current stripe, use the cached objects.
    if (stripeIndex == currentStripe) {
      stripeFooter = this.stripeFooter;
      indexes = indexes == null ? this.indexes : indexes;
      bloomFilterIndex = bloomFilterIndex == null ? this.bloomFilterIndices : bloomFilterIndex;
    }
    return metadata.readRowIndex(stripe, stripeFooter, included, indexes, null,
        bloomFilterIndex);
  }

  private void seekToRowEntry(TreeReaderFactory.TreeReader reader, int rowEntry)
      throws IOException {
    PositionProvider[] index = new PositionProvider[indexes.length];
    for (int i = 0; i < indexes.length; ++i) {
      if (indexes[i] != null) {
        index[i] = new PositionProviderImpl(indexes[i].getEntry(rowEntry));
      }
    }
    reader.seek(index);
  }

  public void seekToRow(long rowNumber) throws IOException {
    if (rowNumber < 0) {
      throw new IllegalArgumentException("Seek to a negative row number " +
          rowNumber);
    } else if (rowNumber < firstRow) {
      throw new IllegalArgumentException("Seek before reader range " +
          rowNumber);
    }
    // convert to our internal form (rows from the beginning of slice)
    rowNumber -= firstRow;

    // move to the right stripe
    int rightStripe = findStripe(rowNumber);
    if (rightStripe != currentStripe) {
      currentStripe = rightStripe;
      readStripe();
    }
    readRowIndex(currentStripe, included);

    // if we aren't to the right row yet, advance in the stripe.
    advanceToNextRow(reader, rowNumber, true);
  }

  @Override
  public void close() throws IOException {
    clearStreams();
    dataReader.close();
  }
}
