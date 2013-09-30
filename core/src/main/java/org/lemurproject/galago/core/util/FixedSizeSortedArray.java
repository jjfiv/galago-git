/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.util;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Fixed size sorted array for document retrieval
 *
 * @author sjh
 */
public class FixedSizeSortedArray<T> {

  private Comparator<T> _cmp;
  private T[] _arr;
  private int _position;

  public FixedSizeSortedArray(Class<T> c, int requested, Comparator<T> cmp) {
    assert (requested > 0);
    this._arr = (T[]) Array.newInstance(c, requested);
    this._position = 0;
    this._cmp = cmp;
  }

  public int size() {
    return _position;
  }

  public T get(int pos) {
    return _arr[pos];
  }

  public T getFinal() {
    if (_position > 0) {
      return _arr[_position - 1];
    } else {
      return null;
    }
  }

  /**
   * Adds an item to the array IFF the heaps is small OR the min-item is worse
   * than this item
   */
  public void offer(T d) {
    // if we're small
    if (_position < _arr.length) {
      _arr[_position] = d;
      _position++;
      bubbleUp(_position - 1);


      // or if smallest item is worse than this document
    } else if (_cmp.compare(d, _arr[_position - 1]) > 0) {
      _arr[_position - 1] = d;
      bubbleUp(_position - 1);
    }
  }

  public T[] getSortedArray() {
    T[] data = (T[]) Arrays.copyOf(_arr, _position);
    // Arrays.sort(data, _cmp);
    return data;
  }

  private void bubbleUp(int pos) {
    int prev = pos - 1;
    while ((prev >= 0) && (_cmp.compare(_arr[pos], _arr[prev]) > 0)) {
      T p = _arr[prev];
      _arr[prev] = _arr[pos];
      _arr[pos] = p;
      pos = prev;
      prev = pos - 1;
    }
  }
}
