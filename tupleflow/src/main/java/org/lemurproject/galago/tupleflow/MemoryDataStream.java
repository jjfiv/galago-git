/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.utility.buffer.DataStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class MemoryDataStream extends DataStream {
    byte[] data;
    int offset;
    int length;
    DataInputStream input;
    
    public MemoryDataStream(byte[] data, int offset, int length) {
        assert data != null;
        assert data.length >= offset + length;
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.input = new DataInputStream(new ByteArrayInputStream(data, offset, length));
    }
    
    public MemoryDataStream subStream(long subOffset, long subLength) {
        assert subOffset <= length;
        assert subOffset + subLength <= length;
        return new MemoryDataStream(
                data, (int) (offset + subOffset),
                (int) subLength);
    }

    public long getPosition() {
        try {
            return length - input.available();
        } catch (IOException ex) {
            return length;
        }
    }

    public boolean isDone() {
        try {
            return input.available() == 0;
        } catch (IOException ex) {
            return true;
        }
    }

    public long length() {
        return length;
    }

    public void seek(long offset) {
        if (offset >= length)
            return;

        try {
            int needToSkip = (int) (offset - getPosition());
            while (needToSkip > 0) {
                int skipped = (int) input.skip(needToSkip);
                needToSkip -= skipped;
            }
        } catch(IOException e) {
        }
    }

    public void readFully(byte[] b) throws IOException {
        input.readFully(b);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        input.readFully(b, off, len);
    }

    public int skipBytes(int n) throws IOException {
        return input.skipBytes(n);
    }

    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    public byte readByte() throws IOException {
        return input.readByte();
    }

    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    public short readShort() throws IOException {
        return input.readShort();
    }

    public int readUnsignedShort() throws IOException {
        return input.readUnsignedShort();
    }

    public char readChar() throws IOException {
        return input.readChar();
    }

    public int readInt() throws IOException {
        return input.readInt();
    }

    public long readLong() throws IOException {
        return input.readLong();
    }

    public float readFloat() throws IOException {
        return input.readFloat();
    }

    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Deprecated
    public String readLine() throws IOException {
        return input.readLine();
    }

    public String readUTF() throws IOException {
        return input.readUTF();
    }
}
