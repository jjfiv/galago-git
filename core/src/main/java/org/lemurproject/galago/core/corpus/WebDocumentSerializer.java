package org.lemurproject.galago.core.corpus;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * This class only stores the raw text, and expects to pass the data through your Tokenizer again.
 * @author jfoley.
 */
public class WebDocumentSerializer extends DocumentSerializer {
  static final Logger log = Logger.getLogger(WebDocumentSerializer.class.getName());
  static final int BUFFER_SIZE = 8192;
  final Tokenizer tokenizer;

  public WebDocumentSerializer(Parameters opts) {
    super(opts);
    tokenizer = Tokenizer.instance(opts);
  }

  @Override
  public byte[] toBytes(Document doc) throws IOException {
    ByteArrayOutputStream headerArray = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(headerArray);
    // identifier
    output.writeLong(doc.identifier);

    // name
    byte[] bytes = Utility.fromString(doc.name);
    output.writeInt(bytes.length);
    output.write(bytes);
    output.close();


    ByteArrayOutputStream metadataArray = new ByteArrayOutputStream();
    output = new DataOutputStream(metadataArray);
    // metadata
    if (doc.metadata == null) {
      output.writeInt(0);
    } else {
      output.writeInt(doc.metadata.size());
      for (String key : doc.metadata.keySet()) {
        bytes = Utility.fromString(key);
        output.writeInt(bytes.length);
        output.write(bytes);

        if(doc.metadata.get(key) == null) {
          output.writeInt(0);
        } else {
          bytes = Utility.fromString(doc.metadata.get(key));
          output.writeInt(bytes.length);
          output.write(bytes);
        }
      }
    }
    output.close();


    ByteArrayOutputStream textArray = new ByteArrayOutputStream();
    output = new DataOutputStream(textArray);
    // text
    if (doc.getText() == null) {
      output.writeInt(0);
    } else {
      bytes = Utility.fromString(doc.getText());
      output.writeInt(bytes.length);
      output.write(bytes);
    }
    output.close();


    ByteArrayOutputStream docArray = new ByteArrayOutputStream();
    output = new DataOutputStream(new SnappyOutputStream(docArray));

    output.writeInt(metadataArray.size());
    output.writeInt(textArray.size());

    output.write(headerArray.toByteArray());
    output.write(metadataArray.toByteArray());
    output.write(textArray.toByteArray());

    output.close();

    return docArray.toByteArray();
  }

  @Override
  public Document fromStream(DataInputStream stream, Document.DocumentComponents selection) throws IOException {
    byte[] buffer = new byte[BUFFER_SIZE];
    int blen;
    DataInputStream input = new DataInputStream(new SnappyInputStream(stream));
    Document d = new Document();

    int metadataSize = input.readInt();
    int textSize = input.readInt();

    // identifier
    d.identifier = input.readLong();

    // name
    blen = input.readInt();
    buffer = sizeCheck(buffer, blen);
    input.readFully(buffer, 0, blen);
    d.name = Utility.toString(buffer, 0, blen);

    if (selection.metadata) {
      // metadata
      int metadataCount = input.readInt();
      d.metadata = new HashMap<String,String>(metadataCount);

      for (int i = 0; i < metadataCount; i++) {
        blen = input.readInt();
        buffer = sizeCheck(buffer, blen);
        input.readFully(buffer, 0, blen);
        String key = Utility.toString(buffer, 0, blen);

        blen = input.readInt();
        String value;
        if(blen == 0) {
          value = null;
        } else {
          buffer = sizeCheck(buffer, blen);
          input.readFully(buffer, 0, blen);
          value = Utility.toString(buffer, 0, blen);
        }

        d.metadata.put(key, value);
      }
      // only both skipping if we need to
    } else if (selection.text) {
      input.skip(metadataSize);
    }

    if (selection.text) {
      // text
      blen = input.readInt();

      int start = (selection.subTextStart < 0) ? 0 : selection.subTextStart;
      int maxLen = blen - start;
      if (start > 0) {
        input.skip(start);
      }
      int readLen = (selection.subTextLen > 0 && selection.subTextLen < maxLen) ? selection.subTextLen : maxLen;

      buffer = sizeCheck(buffer, readLen);
      input.readFully(buffer, 0, readLen);
      d.text = Utility.toString(buffer, 0, readLen);

      if (readLen < maxLen) {
        input.skip(maxLen - readLen);
      }
    }
    input.close();

    // give back terms
    if(selection.tokenize) {
      tokenizer.tokenize(d);
    }

    return d;
  }

  private static byte[] sizeCheck(byte[] currentBuffer, int sz) {
    if (sz > currentBuffer.length) {
      return new byte[sz];
    } else {
      return currentBuffer;
    }
  }
}
