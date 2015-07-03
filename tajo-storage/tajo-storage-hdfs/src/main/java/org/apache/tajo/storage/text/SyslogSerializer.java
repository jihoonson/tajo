package org.apache.tajo.storage.text;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.io.OutputStream;

public class SyslogSerializer extends TextLineSerializer {

  public SyslogSerializer(Schema schema, TableMeta meta) {
    super(schema, meta);
  }

  @Override
  public void init() {

  }

  @Override
  public int serialize(OutputStream out, Tuple input) throws IOException {
    return 0;
  }

  @Override
  public void release() {

  }
}
