// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import java.util.Arrays;

/**
 * (9/30/2011) - Refactored to remove useless boxing of the extent data.
 * Saves a ton on object allocation and overall space utilization.
 *
 * @author irmarc
 */
public class ExtentArray {

  int[] _begins;
  int[] _ends;
  int _position;
  int document;

  public ExtentArray(int capacity) {
    _begins = new int[capacity];
    _ends = null; // lazy load these
    _position = 0;
    document = -1; // not valid yet
  }

  public ExtentArray() {
    this(16);
  }

  private void makeRoom() {
    _begins = Arrays.copyOf(_begins, _begins.length * 2);
    if (_ends != null) _ends = Arrays.copyOf(_ends, _ends.length * 2);
  }

  public void setDocument(int d) {
    document = d;
  }

  public int getDocument() {
    return document;
  }

  public int capacity() {
    return _begins.length;
  }

  public void add(int begin) {
    if (_position == _begins.length) {
      makeRoom();
    }

    _begins[_position] = begin;
    _position += 1;
  }

  public void add(int begin, int end) {
    if (_position == _begins.length) {
      makeRoom();
    }

    _begins[_position] = begin;
    if (_ends == null && _position == 0) _ends = new int[_begins.length];
    _ends[_position] = end;
    _position += 1;
  }

  public int begin(int index) {
    return _begins[index];
  }

  public int end(int index) {
    if (_ends == null) return _begins[index]+1;
    return _ends[index];
  }

  public int size() {
    return _position;
  }

  public void reset() {
    _position = 0;
  }
  
  public String toString(){
    return String.format("ExtentArray:doc=%d:count=%d", document, _position);
  }
}
