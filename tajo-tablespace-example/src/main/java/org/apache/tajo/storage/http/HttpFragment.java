package org.apache.tajo.storage.http;

import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.storage.fragment.Fragment;

import java.net.URI;

public class HttpFragment implements Fragment {
  private final URI uri;
  private final String tableName;
  private final HttpFragmentKey startKey;
  private final HttpFragmentKey endKey;
  private final String[] hosts;

  public HttpFragment(URI uri, String tableName, long startKey, long endKey, String[] hosts) {
    this.uri = uri;
    this.tableName = tableName;
    this.startKey = new HttpFragmentKey(startKey);
    this.endKey = new HttpFragmentKey(endKey);
    this.hosts = hosts;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public FragmentProto getProto() {
    return null;
  }

  @Override
  public HttpFragmentKey getStartKey() {
    return startKey;
  }

  @Override
  public HttpFragmentKey getEndKey() {
    return endKey;
  }

  @Override
  public long getLength() {
    return endKey.getKey() - startKey.getKey();
  }

  @Override
  public String[] getHosts() {
    return hosts;
  }

  @Override
  public boolean isEmpty() {
    return endKey.getKey() - startKey.getKey() == 0;
  }
}
