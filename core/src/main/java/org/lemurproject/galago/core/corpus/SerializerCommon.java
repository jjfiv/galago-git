package org.lemurproject.galago.core.corpus;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jfoley.
 */
public class SerializerCommon {

  /** Shared wrapper around a byte[] in order to limit allocations */
  public static class ByteBuf {
    public static final int BUFFER_SIZE = 8192;
    public byte[] data;
    public ByteBuf(int size) {
      data = new byte[size];
    }
    public ByteBuf() {
      this(BUFFER_SIZE);
    }

    /** use this internal buffer to read into a String object; assumes next item in string is an integer length, followed by the string */
    public String readString(DataInputStream input) throws IOException {
      int length = input.readInt();
      if(length < 0) return null;
      if(length == 0) return "";
      return readString(input, length);
    }

    /** use this internal buffer to read into a String object; reads the next length bytes as a UTF-8 string */
    public String readString(DataInputStream input, int length) throws IOException {
      reserve(length);
      input.readFully(data, 0, length);
      return ByteUtil.toString(data, length);
    }


    /** make sure the internal buffer can hold at least length bytes */
    public void reserve(int length) {
      if(data.length >= length) return;
      data = new byte[length];
    }
  }


  public static void writeString(DataOutputStream output, String text) throws IOException {
    if(text == null) {
      output.writeInt(-1);
      return;
    }
    byte[] bytes = ByteUtil.fromString(text);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  public static ByteArrayOutputStream writeText(Document doc) throws IOException {
    DataOutputStream output;
    byte[] bytes;
    ByteArrayOutputStream textArray = new ByteArrayOutputStream();
    output = new DataOutputStream(textArray);
    // text
    writeString(output, doc.text);
    output.close();
    return textArray;
  }

  public static ByteArrayOutputStream writeMetadata(Document doc) throws IOException {
    DataOutputStream output;
    ByteArrayOutputStream metadataArray = new ByteArrayOutputStream();
    output = new DataOutputStream(metadataArray);
    // metadata
    if (doc.metadata == null) {
      output.writeInt(-1);
    } else {
      output.writeInt(doc.metadata.size());
      for (String key : doc.metadata.keySet()) {
        writeString(output, key);
        writeString(output, doc.metadata.get(key));
      }
    }
    output.close();
    return metadataArray;
  }

  public static ByteArrayOutputStream writeTerms(Document doc) throws IOException {
    DataOutputStream output;
    ByteArrayOutputStream textArray = new ByteArrayOutputStream();
    output = new DataOutputStream(textArray);
    if(doc.terms == null) {
      output.writeInt(-1);
    } else {
      output.writeInt(doc.terms.size());
      for (String term : doc.terms) {
        writeString(output, term);
      }
    }
    output.close();
    return textArray;
  }

  public static String readText(DataInputStream input, Document.DocumentComponents selection, ByteBuf buffer) throws IOException {
    final int textLen = input.readInt();
    if(textLen < 0) return null;

    // handle offset into text
    final int start = Math.max(0, selection.subTextStart);
    if(start > textLen) return "";

    int len = textLen - start;
    if(selection.subTextLen > 0) {
      len = Math.min(len, selection.subTextLen);
    }

    // skip
    if(start > 0) input.skip(start);

    String output = buffer.readString(input, len);

    // move past rest
    if (len < textLen) {
      input.skip(len - textLen);
    }

    return output;
  }

  public static Map<String,String> readMetadata(DataInputStream input, ByteBuf buffer) throws IOException {
    // metadata
    int metadataCount = input.readInt();
    Map<String,String> metadata = new HashMap<String,String>(metadataCount);

    for (int i = 0; i < metadataCount; i++) {
      String key = buffer.readString(input);
      String value = buffer.readString(input);
      metadata.put(key, value);
    }

    return metadata;
  }
}
