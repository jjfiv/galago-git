// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.DataInput;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class VByteInput implements DataInput {
    DataInput input;

    public VByteInput(DataInput input) {
        this.input = input;
    }

    @Override
    public void readFully(byte[] b, int i, int i0) throws IOException {
        input.readFully(b, i, i0);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        input.readFully(b);
    }
    
    public String readString() throws IOException {
        int length = readInt();
        byte[] data = new byte[length];
        input.readFully(data);
        return Utility.toString(data);
    }

    @Override
    public int skipBytes(int i) throws IOException {
        return input.skipBytes(i);
    }

    @Override
    public int readUnsignedShort() throws IOException {
        int result = readInt();
        return (int) (result & 0xffff);
    }

    @Override
    public boolean readBoolean() throws IOException {
        int result = readInt();
        return result != 0 ? true : false;
    }

    @Override
    public byte readByte() throws IOException {
        int result = readInt();
        return (byte) (result & 0xff);
    }

    @Override
    public char readChar() throws IOException {
        int result = readInt();
        return (char) (result & 0xffff);
    }

    @Override
    public double readDouble() throws IOException {
        long result = input.readLong();
        return Double.longBitsToDouble(result);
    }

    @Override
    public float readFloat() throws IOException {
        int result = input.readInt();
        return Float.intBitsToFloat(result);
    }

    @Override
    public int readInt() throws IOException {
      return Utility.uncompressInt(input);
    }

    @Override
    public long readLong() throws IOException {
      return Utility.uncompressLong(input);
    }

    @Override
    public String readLine() throws IOException {
        throw new IOException("Operation not supported.");
    }

    @Override
    public short readShort() throws IOException {
        return (short) (readInt() & 0xffff);
    }

    @Override
    public String readUTF() throws IOException {
        throw new IOException("Operation not supported.");
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return (readInt() & 0xff);
    }
}
