package org.lemurproject.galago.core.corpus;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.utility.Parameters;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * This class only stores the raw text, and expects to pass the data through your Tokenizer again.
 * @author jfoley.
 */
public class WebDocumentSerializer extends DocumentSerializer {
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
    SerializerCommon.writeString(output, doc.name);

    ByteArrayOutputStream metadataArray = SerializerCommon.writeMetadata(doc);
    ByteArrayOutputStream textArray = SerializerCommon.writeText(doc);

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
    int textSize = input.readInt(); // ignored

    // identifier
    d.identifier = input.readLong();

    // name
    d.name = SerializerCommon.readString(input, buffer);

    if (selection.metadata) {
      d.metadata = SerializerCommon.readMetadata(input, buffer);
      // only both skipping if we need to
    } else if (selection.text || selection.tokenize) {
      input.skip(metadataSize);
    }

    // can't get tokens without text in this case...
    if (selection.text || selection.tokenize) {
      d.text = SerializerCommon.readText(input, selection, buffer);
    }
    input.close();

    // give back terms & tags
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
