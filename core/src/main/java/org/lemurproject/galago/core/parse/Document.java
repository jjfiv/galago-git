// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class Document implements Serializable {
  static final int BUFFER_SIZE = 5000;

  // document id - this value is serialized
  public int identifier = -1;
  // document data - these values are serialized
  public String name;
  public Map<String, String> metadata;
  public String text;
  public List<String> terms;
  public List<Tag> tags;
  // other data - used to generate an identifier; these values can not be serialized!
  public int fileId = -1;
  public int totalFileCount = -1;

  public Document() {
    metadata = new HashMap();
  }

  public Document(String externalIdentifier, String text) {
    this();
    this.name = externalIdentifier;
    this.text = text;
  }

  public Document(Document d) {
    this.identifier = d.identifier;
    this.name = d.name;
    this.metadata = new HashMap(d.metadata);
    this.text = d.text;
    this.terms = new ArrayList(d.terms);
    this.tags = new ArrayList(d.tags);
    this.fileId = d.fileId;
    this.totalFileCount = d.totalFileCount;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Identifier: ").append(name).append("\n");
    if (metadata != null) {
      sb.append("Metadata: \n");
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        sb.append("<");
        sb.append(entry.getKey()).append(",").append(entry.getValue());
        sb.append("> ");
      }
    }

    if (tags != null) {
      sb.append("\nTags: \n");
      for (Tag t : tags) {
        sb.append(t).append(" ");
      }
    }

    if (terms != null) {
      sb.append("\nTerm vector: \n");
      for (String s : terms) {
        sb.append(s).append(" ");
      }
    }

    if (text != null) {
      sb.append("\nText :").append(text);
    }
    sb.append("\n");
    
    return sb.toString();
  }

  public static byte[] serialize(Parameters p, Document doc) throws IOException {
    ByteArrayOutputStream headerArray = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(headerArray);
    // identifier
    output.writeInt(doc.identifier);

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

        bytes = Utility.fromString(doc.metadata.get(key));
        output.writeInt(bytes.length);
        output.write(bytes);
      }
    }
    output.close();


    ByteArrayOutputStream textArray = new ByteArrayOutputStream();
    output = new DataOutputStream(textArray);
    // text
    if (doc.text == null) {
      output.writeInt(0);
    } else {
      bytes = Utility.fromString(doc.text);
      output.writeInt(bytes.length);
      output.write(bytes);
    }
    output.close();


    ByteArrayOutputStream tagsArray = new ByteArrayOutputStream();
    output = new DataOutputStream(tagsArray);
    // tags
    if (doc.tags == null) {
      output.writeInt(0);
    } else {
      output.writeInt(doc.tags.size());
      for (Tag tag : doc.tags) {
        bytes = Utility.fromString(tag.name);
        output.writeInt(bytes.length);
        output.write(bytes);
        output.writeInt(tag.begin);
        output.writeInt(tag.end);
        output.writeInt(tag.attributes.size());
        for (String key : tag.attributes.keySet()) {
          bytes = Utility.fromString(key);
          output.writeInt(bytes.length);
          output.write(bytes);
          bytes = Utility.fromString(tag.attributes.get(key));
          output.writeInt(bytes.length);
          output.write(bytes);
        }
      }
    }
    output.close();


    ByteArrayOutputStream termsArray = new ByteArrayOutputStream();
    output = new DataOutputStream(termsArray);

    // terms
    if (doc.terms == null) {
      output.writeInt(0);
    } else {
      output.writeInt(doc.terms.size());
      for (String term : doc.terms) {
        bytes = Utility.fromString(term);
        output.writeInt(bytes.length);
        output.write(bytes);
      }
    }

    output.close();

    ByteArrayOutputStream docArray = new ByteArrayOutputStream();
    output = new DataOutputStream(new SnappyOutputStream(docArray));

    output.writeInt(metadataArray.size());
    output.writeInt(textArray.size());
    output.writeInt(tagsArray.size());
    output.writeInt(termsArray.size());

    output.write(headerArray.toByteArray());
    output.write(metadataArray.toByteArray());
    output.write(textArray.toByteArray());
    output.write(tagsArray.toByteArray());
    output.write(termsArray.toByteArray());

    output.close();

    return docArray.toByteArray();
  }

  public static Document deserialize(byte[] data, Parameters p) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    return deserialize(new DataInputStream(stream), p);
  }

  public static Document deserialize(DataInputStream stream, Parameters p) throws IOException {
    byte[] buffer = new byte[BUFFER_SIZE];
    int blen;
    DataInputStream input = new DataInputStream(new SnappyInputStream(stream));
    Document d = new Document();

    int metadataSize = input.readInt();
    int textSize = input.readInt();
    int tagsSize = input.readInt();
    int termsSize = input.readInt();

    // identifier
    d.identifier = input.readInt();

    // name
    blen = input.readInt();
    buffer = sizeCheck(buffer, blen);
    input.readFully(buffer, 0, blen);
    d.name = Utility.toString(buffer, 0, blen);

    if (p.get("metadata", true)) {
      // metadata
      int metadataCount = input.readInt();
      d.metadata = new HashMap(metadataCount);
      
      for (int i = 0; i < metadataCount; i++) {
	blen = input.readInt();
	buffer = sizeCheck(buffer, blen);
        input.readFully(buffer, 0, blen);
        String key = Utility.toString(buffer, 0, blen);
	
	blen = input.readInt();
	buffer = sizeCheck(buffer, blen);
        input.readFully(buffer, 0, blen);
        String value = Utility.toString(buffer, 0, blen);

        d.metadata.put(key, value);
      }
      // only both skipping if we need to
    } else if (p.get("terms", true) || p.get("text", true) || p.get("tags", true)) {
      input.skip(metadataSize);
    }

    if (p.get("text", true)) {
      // text
      blen = input.readInt();
      buffer = sizeCheck(buffer, blen);
      input.readFully(buffer, 0, blen);
      d.text = Utility.toString(buffer, 0, blen);

      // only both skipping if we need to
    } else if (p.get("terms", true) || p.get("tags", true)) {
      input.skip(textSize);
    }

    if (p.get("tags", true)) {
      // tags
      int tagCount = input.readInt();
      d.tags = new ArrayList(tagCount);
      for (int i = 0; i < tagCount; i++) {
	blen = input.readInt();
        buffer = sizeCheck(buffer, blen);
        input.readFully(buffer, 0, blen);
        String tagName = Utility.toString(buffer, 0, blen);
        int tagBegin = input.readInt();
        int tagEnd = input.readInt();
        HashMap<String, String> attributes = new HashMap();
        int attrCount = input.readInt();
        for (int j = 0; j < attrCount; j++) {
	  blen = input.readInt();
	  buffer = sizeCheck(buffer, blen);
          input.readFully(buffer, 0, blen);
          String key = Utility.toString(buffer, 0, blen);

	  blen = input.readInt();
	  buffer = sizeCheck(buffer, blen);
          input.readFully(buffer, 0, blen);
          String value = Utility.toString(buffer, 0, blen);

          attributes.put(key, value);
        }
        Tag t = new Tag(tagName, attributes, tagBegin, tagEnd);
        d.tags.add(t);
      }

      // only both skipping if we need to
    } else if (p.get("terms", true)) {
      input.skip(tagsSize);
    }

    if (p.get("terms", true)) {
      // terms
      int termCount = input.readInt();
      d.terms = new ArrayList(termCount);
      if (termCount > 10000) {
	  System.err.printf("Reading in %d terms of document %d, %s.\n", termCount, d.identifier, d.name);
      }
      for (int i = 0; i < termCount; i++) {
	blen = input.readInt();
	buffer = sizeCheck(buffer, blen);
        input.readFully(buffer, 0, blen);
        d.terms.add(Utility.toString(buffer, 0, blen));
      }
    }

    input.close();
    return d;
  }

  protected static byte[] sizeCheck(byte[] currentBuffer, int sz) {
      if (sz > currentBuffer.length) {
	  return new byte[sz];
      } else {
	  return currentBuffer;
      }
  }
}
