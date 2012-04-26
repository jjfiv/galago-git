// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class Document implements Serializable {

  // document id - this values are serialized
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

  public Document(String identifier, String text) {
    this();
    this.name = identifier;
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

    if (terms != null) {
      sb.append("Term vector: \n");
      for (String s : terms) {
        sb.append(s).append(" ");
      }
    }
    sb.append("\n");
    if (text != null) {
      sb.append("Text :").append(text);
    }
    return sb.toString();
  }

  public static byte[] serialize(Parameters p, Document doc) throws IOException {
    ByteArrayOutputStream array = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(new SnappyOutputStream(array));

    boolean metadata = p.get("corpusMetadata", true);
    boolean text = p.get("corpusText", true);
    boolean terms = p.get("corpusTerms", true);
    boolean tags = p.get("corpusTags", true);

    // options
    output.writeBoolean(metadata);
    output.writeBoolean(text);
    output.writeBoolean(terms);
    output.writeBoolean(tags);

    // identifier
    output.writeInt(doc.identifier);

    // name
    output.writeInt(doc.name.length());
    output.write(Utility.fromString(doc.name));

    if (metadata) {
      // metadata
      output.writeInt(doc.metadata.size());
      for (String key : doc.metadata.keySet()) {
        output.writeInt(key.length());
        output.write(Utility.fromString(key));
        output.writeInt(doc.metadata.get(key).length());
        output.write(Utility.fromString(doc.metadata.get(key)));
      }
    }

    if (text) {
      // text
      output.writeInt(doc.text.length());
      output.write(Utility.fromString(doc.text));
    }

    if (terms) {
      // terms
      output.writeInt(doc.terms.size());
      for (String term : doc.terms) {
        output.writeInt(term.length());
        output.write(Utility.fromString(term));
      }
    }

    if (tags) {
      // tags
      output.writeInt(doc.tags.size());
      for (Tag tag : doc.tags) {
        output.writeInt(tag.name.length());
        output.write(Utility.fromString(tag.name));
        output.writeInt(tag.begin);
        output.writeInt(tag.end);
        output.writeInt(tag.attributes.size());
        for (String key : tag.attributes.keySet()) {
          output.writeInt(key.length());
          output.write(Utility.fromString(key));
          output.writeInt(tag.attributes.get(key).length());
          output.write(Utility.fromString(tag.attributes.get(key)));
        }
      }
    }

    output.close();

    return array.toByteArray();
  }

  public static Document deserialize(byte[] data) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    return deserialize(new DataInputStream(stream));
  }

  public static Document deserialize(DataInputStream stream) throws IOException {
    DataInputStream input = new DataInputStream(new SnappyInputStream(stream));
    Document d = new Document();

    // options
    boolean metadata = input.readBoolean();
    boolean text = input.readBoolean();
    boolean terms = input.readBoolean();
    boolean tags = input.readBoolean();

    // identifier
    d.identifier = input.readInt();

    // name
    byte[] docNameBytes = new byte[input.readInt()];
    input.readFully(docNameBytes);
    d.name = Utility.toString(docNameBytes);

    if (metadata) {
      // metadata
      int metadataCount = input.readInt();
      d.metadata = new HashMap(metadataCount);
      for (int i = 0; i < metadataCount; i++) {

        byte[] keyBytes = new byte[input.readInt()];
        input.readFully(keyBytes);
        String key = Utility.toString(keyBytes);

        byte[] valueBytes = new byte[input.readInt()];
        input.readFully(valueBytes);
        String value = Utility.toString(valueBytes);

        d.metadata.put(key, value);
      }
    }
    if (text) {
      // text
      byte[] textBytes = new byte[input.readInt()];
      input.readFully(textBytes);
      d.text = Utility.toString(textBytes);
    }

    if (terms) {
      // terms
      int termCount = input.readInt();
      d.terms = new ArrayList(termCount);
      for (int i = 0; i < termCount; i++) {
        byte[] termBytes = new byte[input.readInt()];
        input.readFully(termBytes);
        d.terms.add(Utility.toString(termBytes));
      }
    }

    if (tags) {
      // tags
      int tagCount = input.readInt();
      d.tags = new ArrayList(tagCount);
      for (int i = 0; i < tagCount; i++) {
        byte[] tagNameBytes = new byte[input.readInt()];
        input.readFully(tagNameBytes);
        String tagName = Utility.toString(tagNameBytes);
        int tagBegin = input.readInt();
        int tagEnd = input.readInt();
        HashMap<String, String> attributes = new HashMap();
        int attrCount = input.readInt();
        for (int j = 0; j < attrCount; j++) {
          byte[] keyBytes = new byte[input.readInt()];
          input.readFully(keyBytes);
          String key = Utility.toString(keyBytes);
          byte[] valueBytes = new byte[input.readInt()];
          input.readFully(valueBytes);
          String value = Utility.toString(valueBytes);
          attributes.put(key, value);
        }
        Tag t = new Tag(tagName, attributes, tagBegin, tagEnd);
        d.tags.add(t);
      }
    }

    input.close();
    return d;
  }
}
