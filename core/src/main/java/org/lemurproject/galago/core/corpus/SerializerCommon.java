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
  public static final int BUFFER_SIZE = 8192;

  public static void writeString(DataOutputStream output, String text) throws IOException {
    if(text == null) {
      output.writeInt(-1);
      return;
    }
    byte[] bytes = ByteUtil.fromString(text);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  public static String readString(DataInputStream input, byte[] buffer) throws IOException {
    int length = input.readInt();
    if(length < 0) return null;
    System.err.println(length);
    buffer = sizeCheck(buffer, length);
    input.readFully(buffer, 0, length);
    return ByteUtil.toString(buffer, length);
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

  public static String readText(DataInputStream input, Document.DocumentComponents selection, byte[] buffer) throws IOException {
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

    buffer = sizeCheck(buffer, len);
    input.readFully(buffer, 0, len);
    String output = ByteUtil.toString(buffer, 0, len);

    // move past rest
    if (len < textLen) {
      input.skip(len - textLen);
    }

    return output;
  }

  public static Map<String,String> readMetadata(DataInputStream input, byte[] buffer) throws IOException {
    // metadata
    int metadataCount = input.readInt();
    Map<String,String> metadata = new HashMap<String,String>(metadataCount);

    for (int i = 0; i < metadataCount; i++) {
      String key = readString(input, buffer);
      System.out.println("K:"+key);
      String value = readString(input, buffer);
      System.out.println("V:"+value);
      metadata.put(key, value);
    }

    return metadata;
  }

  public static byte[] sizeCheck(byte[] currentBuffer, int sz) {
    if (sz > currentBuffer.length) {
      return new byte[sz];
    } else {
      return currentBuffer;
    }
  }
}
