/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.util;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import org.lemurproject.galago.core.retrieval.ScoredDocument;

/**
 *
 * @author sjh
 */
public class FixedSizeMinHeap<T> {

  private Comparator<T> _cmp;
  private T[] _heap;
  private int _position;

  public FixedSizeMinHeap(Class<T> c, int requested, Comparator<T> cmp) {
    this._heap = (T[]) Array.newInstance(c, requested);
    this._position = 0;
    this._cmp = cmp;
  }

  public int size() {
    return _position;
  }

  public T peek() {
    return _heap[0];
  }

  /**
   * Adds an item to the heap IFF the heaps is small OR the min-item is worse than this item
   */
  public void offer(T d) {
    // if we're small
    if (_position < _heap.length) {
      _heap[_position] = d;
      _position++;
      bubbleUp(_position - 1);


      // or if smallest item is worse than this document
    } else if (_cmp.compare(d, _heap[0]) > 0) {
      _heap[0] = d;
      bubbleDown(0);
    }
  }

  public T[] getSortedArray() {
    T[] data = (T[]) Arrays.copyOf(_heap, _position);
    Arrays.sort(data, Collections.reverseOrder(_cmp));
    return data;
  }

  private void bubbleUp(int pos) {
    int parent = (pos - 1) / 2;
    if (_cmp.compare(_heap[pos], _heap[parent]) < 0) {
      T p = _heap[parent];
      _heap[parent] = _heap[pos];
      _heap[pos] = p;
      bubbleUp(parent);
    }
  }

  private void bubbleDown(int pos) {
    int child1 = (2 * pos) + 1;
    int child2 = child1 + 1;

    int selectedChild = (child1 < _position) ? child1 : -1;

    if (child2 < _position) {
      selectedChild = (_cmp.compare(_heap[child1], _heap[child2]) < 0) ? child1 : child2;
    }

    // the parent is bigger than the child (assuming a child)
    if (selectedChild > 0 && _cmp.compare(_heap[pos], _heap[selectedChild]) > 0) {
      T p = _heap[selectedChild];
      _heap[selectedChild] = _heap[pos];
      _heap[pos] = p;
      bubbleDown(selectedChild);
    }
  }
}
