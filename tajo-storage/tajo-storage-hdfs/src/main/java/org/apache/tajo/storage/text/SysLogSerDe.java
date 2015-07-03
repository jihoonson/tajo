package org.apache.tajo.storage.text;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;

public class SysLogSerDe extends TextLineSerDe{
  @Override
  public TextLineDeserializer createDeserializer(Schema schema, TableMeta meta, Column[] projected) {
    return new SyslogDeserializer(schema, meta);
  }

  @Override
  public TextLineSerializer createSerializer(Schema schema, TableMeta meta) {
    return new SyslogSerializer(schema, meta);
  }
}
