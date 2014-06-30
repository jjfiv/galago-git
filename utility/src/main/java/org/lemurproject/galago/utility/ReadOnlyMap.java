// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility;

import java.util.Map;

/**
 *
 * @author jfoley
 * @param <K>
 * @param <V>
 */
public abstract class ReadOnlyMap<K,V> implements Map<K,V> {
    
  @Override
  public V put(K k, V v) {
    throw new UnsupportedOperationException("Map is read-only.");
  }

  @Override
  public V remove(Object o) {
    throw new UnsupportedOperationException("Map is read-only.");
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    throw new UnsupportedOperationException("Map is read-only.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Map is read-only.");
  }
}
