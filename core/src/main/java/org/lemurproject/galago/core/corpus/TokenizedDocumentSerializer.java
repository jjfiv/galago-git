package org.lemurproject.galago.core.corpus;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * @author jfoley.
 */
public class TokenizedDocumentSerializer extends DocumentSerializer {
  static final Logger log = Logger.getLogger(TokenizedDocumentSerializer.class.getName());
  public TokenizedDocumentSerializer(Parameters opts) {
    super(opts);
  }

  @Override
  public byte[] toBytes(Document doc) throws IOException {
    ByteArrayOutputStream headerArray = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(headerArray);
    // identifier
    output.writeLong(doc.identifier);

    // name
    byte[] bytes = ByteUtil.fromString(doc.name);
    output.writeInt(bytes.length);
    output.write(bytes);
    output.close();

    ByteArrayOutputStream metadataArray = SerializerCommon.writeMetadata(doc);
    ByteArrayOutputStream textArray = SerializerCommon.writeText(doc);
    ByteArrayOutputStream termsArray = SerializerCommon.writeTerms(doc);

    ByteArrayOutputStream docArray = new ByteArrayOutputStream();
    output = new DataOutputStream(new SnappyOutputStream(docArray));


    output.write(headerArray.toByteArray());

    output.writeInt(metadataArray.size());
    output.writeInt(textArray.size());
    output.writeInt(termsArray.size());

    output.write(metadataArray.toByteArray());
    output.write(textArray.toByteArray());
    output.write(termsArray.toByteArray());

    output.close();

    return docArray.toByteArray();
  }

  @Override
  public Document fromStream(DataInputStream stream, Document.DocumentComponents selection) throws IOException {
    SerializerCommon.ByteBuf buffer = new SerializerCommon.ByteBuf();
    DataInputStream input = new DataInputStream(new SnappyInputStream(stream));
    Document d = new Document();

    // identifier
    d.identifier = input.readLong();

    // name
    d.name = buffer.readString(input);

    // exit with no parts
    if(!selection.metadata && !selection.text && !selection.tokenize) return d;

    int metadataSize = input.readInt();
    int textSize = input.readInt();
    int termsSize = input.readInt();

    if (selection.metadata) {
      d.metadata = SerializerCommon.readMetadata(input, buffer);
      // only both skipping if we need to
    } else {
      input.skip(metadataSize);
    }

    if (selection.text) {
      d.text = SerializerCommon.readText(input, selection, buffer);
    } else {
      input.skip(textSize);
    }

    // give back terms
    if(selection.tokenize) {
      int count = input.readInt();
      ArrayList<String> terms = new ArrayList<String>(count);
      for (int i = 0; i < count; i++) {
        terms.add(buffer.readString(input));
      }
      d.terms = terms;
    }
    input.close();

    return d;
  }

}
