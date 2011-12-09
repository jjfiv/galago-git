// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.util;

/**
 *
 * @author trevor
 */
public class FloatArray {
    float[] _array;
    int _position;

    public FloatArray(int capacity) {
        _array = new float[capacity];
        _position = 0;
    }

    public FloatArray() {
        this(16);
    }

    public void add(float value) {
        if(_position == _array.length) {
            // grow array if we're out of space
            _array = _copyArray(_array.length * 2);
        }

        _array[_position] = value;
        _position += 1;
    }

    public float[] getBuffer() {
        return _array;
    }

    public int getPosition() {
        return _position;
    }

    private float[] _copyArray(int newSize) {
        float[] result = new float[newSize];
        System.arraycopy(_array, 0, result, 0, _position);
        return result;
    }

    public float[] toArray() {
        return _copyArray(_position);
    }
}
