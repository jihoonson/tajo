package org.apache.tajo.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class HttpTablespace extends Tablespace {
  private final Log LOG = LogFactory.getLog(HttpTablespace.class);

  private static final StorageProperty FileStorageProperties = new StorageProperty("TEXT", false, false, true);
  private static final FormatProperty GeneralFileProperties = new FormatProperty(false, false, false);

  private LocalFileSystem fs;
  private Path spacePath;

  public HttpTablespace(String spaceName, URI uri) {
    super(spaceName, uri);
  }

  @Override
  protected void storageInit() throws IOException {
    this.spacePath = new Path(uri);
    this.fs = LocalFileSystem.getLocal(conf);
  }

  @Override
  public void setConfig(String name, String value) {
    conf.set(name, value);
  }

  @Override
  public void setConfigs(Map<String, String> configs) {
    for (Map.Entry<String, String> c : configs.entrySet()) {
      conf.set(c.getKey(), c.getValue());
    }
  }

  @Override
  public long getTableVolume(URI uri) throws IOException {
    return 0;
  }

  @Override
  public URI getTableUri(String databaseName, String tableName) {
    // TODO: is this necessary?
    return null;
  }

  @Override
  public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc, ScanNode scanNode) throws IOException, TajoException {
    return null;
  }

  @Override
  public StorageProperty getProperty() {
    return null;
  }

  @Override
  public FormatProperty getFormatProperty(TableMeta meta) {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc, Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange) throws IOException {
    return new TupleRange[0];
  }

  @Override
  public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) throws TajoException {

  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws TajoException, IOException {

  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException, TajoException {

  }

  @Override
  public void prepareTable(LogicalNode node) throws IOException, TajoException {

  }

  @Override
  public Path commitTable(OverridableConf queryContext, ExecutionBlockId finalEbId, LogicalPlan plan, Schema schema, TableDesc tableDesc) throws IOException {
    return null;
  }

  @Override
  public void rollbackTable(LogicalNode node) throws IOException, TajoException {

  }

  @Override
  public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
    return null;
  }
}
