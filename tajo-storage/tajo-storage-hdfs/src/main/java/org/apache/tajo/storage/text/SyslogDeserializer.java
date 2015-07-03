package org.apache.tajo.storage.text;

import io.netty.buffer.ByteBuf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

public class SyslogDeserializer extends TextLineDeserializer {

  public SyslogDeserializer(Schema schema, TableMeta meta) {
    super(schema, meta);
  }

  @Override
  public void init() {

  }

  @Override
  public void deserialize(ByteBuf lineBuf, Tuple output) throws IOException, TextLineParsingError {
    byte[] bytes = new byte[lineBuf.readableBytes()];
    lineBuf.readBytes(bytes);
    String line = new String(bytes);
    String[] tokens = line.split(" ");
    Datum[] datums = new Datum[schema.size()];
    datums[0] = DatumFactory.createTimestamp("2015 " + tokens[0] + " " + tokens[1] + " " + tokens[2]);
    datums[1] = DatumFactory.createText(tokens[3]);
    datums[2] = DatumFactory.createText(tokens[4]);
    StringBuilder sb = new StringBuilder();
    for (int i = 6; i < tokens.length; i++) {
      sb.append(tokens[i]);
    }
    datums[4] = DatumFactory.createText(sb.toString());
    output.put(datums);
  }

  @Override
  public void release() {

  }
}
