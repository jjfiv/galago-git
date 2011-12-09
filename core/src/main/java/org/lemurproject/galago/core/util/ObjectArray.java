// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import java.lang.reflect.Array;

public class ObjectArray<T> {

  Class<T> myClass;
  T[] _array;
  int _position;

  public ObjectArray(Class<T> c, int capacity) {
    myClass = c;
    _array = (T[]) Array.newInstance(myClass, capacity);
    _position = 0;
  }

  //must explicitly supply the class at instantiation  
  public ObjectArray(Class<T> c) {
    this(c, 16);
  }

  public void add(T value) {
    if (_position == _array.length) {
      // grow array if we're out of space
      _array = _copyArray(_array.length * 2);
    }

    _array[_position] = value;
    _position += 1;
  }

  public T[] getBuffer() {
    return (T[]) _array;
  }

  public int getPosition() {
    return _position;
  }

  private T[] _copyArray(int newSize) {
    T[] result = (T[]) Array.newInstance(myClass, newSize);
    System.arraycopy(_array, 0, result, 0, _position);
    return result;
  }

  public T[] toArray() {
    return _copyArray(_position);
  }
}
