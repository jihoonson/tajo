package org.apache.tajo.storage.http;

import org.apache.tajo.storage.fragment.FragmentKey;

public class HttpFragmentKey implements FragmentKey {
  private long key;

  public HttpFragmentKey(long key) {
    this.key = key;
  }

  public void setKey(long key) {
    this.key = key;
  }

  public long getKey() {
    return key;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(key);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof HttpFragmentKey) {
      HttpFragmentKey other = (HttpFragmentKey) o;
      return this.key == other.key;
    }
    return false;
  }
}
