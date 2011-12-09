// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.util;

public class IntArray {
    int[] _array;
    int _position;

    public IntArray(int capacity) {
        _array = new int[capacity];
        _position = 0;
    }

    public IntArray() {
        this(16);
    }

    public void add(int value) {
        if(_position == _array.length) {
            // grow array if we're out of space
            _array = _copyArray(_array.length * 2);
        }

        _array[_position] = value;
        _position += 1;
    }

    public int[] getBuffer() {
        return _array;
    }

    public int getPosition() {
        return _position;
    }

    private int[] _copyArray(int newSize) {
        int[] result = new int[newSize];
        System.arraycopy(_array, 0, result, 0, _position);
        return result;
    }

    public int[] toArray() {
        return _copyArray(_position);
    }
}
