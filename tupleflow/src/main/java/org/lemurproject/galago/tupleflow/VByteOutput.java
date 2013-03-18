// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class VByteOutput implements DataOutput {

  DataOutput output;

  /**
   * Creates a new instance of VByteOutput
   */
  public VByteOutput(DataOutput output) {
    this.output = output;
  }

  public void writeUTF(String string) throws IOException {
    writeInt(string.length());
    output.writeUTF(string);
  }

  public void writeBytes(String string) throws IOException {
    writeInt(string.length());
    output.writeBytes(string);
  }

  public void writeChars(String string) throws IOException {
    writeInt(string.length());
    output.writeChars(string);
  }

  public void writeString(String string) throws IOException {
    byte[] bytes = Utility.fromString(string);
    writeInt(bytes.length);
    write(bytes);
  }

  public void write(byte[] b, int i, int i0) throws IOException {
    output.write(b, i, i0);
  }

  public void write(byte[] b) throws IOException {
    output.write(b);
  }

  public void write(int i) throws IOException {
    Utility.compressInt(output, i);
  }

  public void writeByte(int i) throws IOException {
    write(i);
  }

  public void writeChar(int i) throws IOException {
    write(i);
  }

  public void writeInt(int i) throws IOException {
    write(i);
  }

  public void writeShort(int i) throws IOException {
    write(i);
  }

  public void writeBoolean(boolean b) throws IOException {
    if (b) {
      write(1);
    } else {
      write(0);
    }
  }

  public void writeLong(long i) throws IOException {
    Utility.compressLong(output, i);
  }

  public void writeDouble(double d) throws IOException {
    output.writeLong(Double.doubleToRawLongBits(d));
  }

  public void writeFloat(float f) throws IOException {
    output.writeInt(Float.floatToRawIntBits(f));
  }
}
