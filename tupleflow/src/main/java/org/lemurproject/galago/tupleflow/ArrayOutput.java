// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class ArrayOutput {

  DataOutput output;

  public ArrayOutput(DataOutput o) {
    output = o;
  }

  public DataOutput getDataOutput() {
    return output;
  }

  public void writeInt(int out) throws IOException {
    output.writeInt(out);
  }

  public void writeInts(int[] out) throws IOException {
    output.writeInt(out.length);
    for (int i = 0; i < out.length; i++) {
      output.writeInt(out[i]);
    }
  }

  public void writeLong(long out) throws IOException {
    output.writeLong(out);
  }

  public void writeLongs(long[] out) throws IOException {
    output.writeInt(out.length);
    for (int i = 0; i < out.length; i++) {
      output.writeLong(out[i]);
    }
  }

  public void writeChar(char out) throws IOException {
    output.writeChar(out);
  }

  public void writeChars(char[] out) throws IOException {
    output.writeInt(out.length);
    for (int i = 0; i < out.length; i++) {
      output.writeChar(out[i]);
    }
  }

  public void writeBoolean(boolean out) throws IOException {
    byte b = out ? (byte) 1 : (byte) 0;
    output.writeByte(b);
  }

  public void writeByte(byte out) throws IOException {
    output.writeByte(out);
  }

  public void writeBytes(byte[] out) throws IOException {
    output.writeInt(out.length);
    output.write(out);
  }

  public void writeShort(short out) throws IOException {
    output.writeShort(out);
  }

  public void writeShorts(short[] out) throws IOException {
    output.writeInt(out.length);
    for (int i = 0; i < out.length; i++) {
      output.writeShort(out[i]);
    }
  }

  public void writeDouble(double out) throws IOException {
    output.writeDouble(out);
  }

  public void writeDoubles(double[] out) throws IOException {
    output.writeInt(out.length);
    for (int i = 0; i < out.length; i++) {
      output.writeDouble(out[i]);
    }
  }

  public void writeFloat(float out) throws IOException {
    output.writeFloat(out);
  }

  public void writeFloats(float[] out) throws IOException {
    output.writeInt(out.length);
    for (int i = 0; i < out.length; i++) {
      output.writeFloat(out[i]);
    }
  }

  public void writeString(String out) throws IOException {
    // check to see if there are any UTF-8 chars in here
    byte[] bytes = new byte[out.length()];

    for (int i = 0; i < out.length(); i++) {
      char c = out.charAt(i);
      if (c >= 128) {
        bytes = out.getBytes("UTF-8");
        break;
      } else {
        bytes[i] = (byte) c;
      }
    }

    writeBytes(bytes);
  }

  public void writeStrings(String[] out) throws IOException {
    output.writeInt(out.length);
    for (int i = 0; i < out.length; i++) {
      writeString(out[i]);
    }
  }
}
