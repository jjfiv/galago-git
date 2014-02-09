
package org.lemurproject.galago.tupleflow.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This is an abstract class for wrapping a particular map type.
 * Useful for when classes are a glorified map.
 * 
 * @see QueryJudgments
 * @author jfoley
 * @param <K>
 * @param <V>
 */
public class WrappedMap<K,V> implements Map<K,V> {
  protected Map<K, V> wrapped;
  
  public WrappedMap(Map<K,V> internal) {
    this.wrapped = internal;
  }
  
  public final Map<K,V> getWrapped() {
    return this.wrapped;
  }
  public final void updateWrapped(Map<K,V> newMap) {
    this.wrapped = newMap;
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  @Override
  public boolean isEmpty() {
    return wrapped.isEmpty();
  }

  @Override
  public boolean containsKey(Object o) {
    return wrapped.containsKey(o);
  }

  @Override
  public boolean containsValue(Object o) {
    return wrapped.containsValue(o);
  }

  @Override
  public V get(Object o) {
    return wrapped.get(o);
  }

  @Override
  public V put(K k, V v) {
    return wrapped.put(k,v);
  }

  @Override
  public V remove(Object o) {
    return wrapped.remove(o);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    wrapped.putAll(map);
  }

  @Override
  public void clear() {
    wrapped.clear();
  }

  @Override
  public Set<K> keySet() {
    return wrapped.keySet();
  }

  @Override
  public Collection<V> values() {
    return wrapped.values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return wrapped.entrySet();
  }
}

