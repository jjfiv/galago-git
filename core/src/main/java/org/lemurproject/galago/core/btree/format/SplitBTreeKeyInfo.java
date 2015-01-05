package org.lemurproject.galago.core.btree.format;

import org.lemurproject.galago.core.btree.simple.DiskMapWrapper;

import java.io.*;

/**
 * @author jfoley.
 */
public class SplitBTreeKeyInfo {
  int valueOutputId; // file number;
  long valueOffset; // offset in file.
  long valueLength; // length of value in file.

  public byte[] toBytes() {
    return codec.toBytes(this);
  }

  public static final DiskMapWrapper.Codec<SplitBTreeKeyInfo> codec = new DiskMapWrapper.Codec<SplitBTreeKeyInfo>() {
    @Override
    public SplitBTreeKeyInfo fromBytes(byte[] in) {
      try {
        DataInput bais = new DataInputStream(new ByteArrayInputStream(in));
        SplitBTreeKeyInfo info = new SplitBTreeKeyInfo();
        info.valueOutputId = bais.readInt();
        info.valueOffset = bais.readLong();
        info.valueLength = bais.readLong();
        return info;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public byte[] toBytes(SplitBTreeKeyInfo out) {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(out.valueOutputId);
        dos.writeLong(out.valueOffset);
        dos.writeLong(out.valueLength);
        dos.flush();
        dos.close();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  };
}
