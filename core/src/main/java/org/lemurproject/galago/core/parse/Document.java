// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Document implements Serializable {

  // document id
  public int identifier = -1;
  // document data
  public String name;
  public Map<String, String> metadata;
  public String text;
  public List<String> terms = null;
  public List<Tag> tags = null;
  // other data - used to generate an identifier
  public int fileId = -1;
  public int totalFileCount = -1;

  public Document() {
    this.metadata = new HashMap<String, String>();
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

  public static byte[] serialize(Document doc, boolean compressed) throws IOException {
    ByteArrayOutputStream array = new ByteArrayOutputStream();
    ObjectOutputStream output;
    if (compressed) {
      output = new ObjectOutputStream(new GZIPOutputStream(array));
    } else {
      output = new ObjectOutputStream(array);
    }

    output.writeObject(doc);
    output.close();

    return array.toByteArray();
  }

  public static Document deserialize(byte[] data, boolean compressed) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    ObjectInputStream docInput;
    if (compressed) {
      docInput = new ObjectInputStream(new GZIPInputStream(stream));
    } else {
      docInput = new ObjectInputStream(stream);
    }

    Document document = null;
    try {
      document = (Document) docInput.readObject();
    } catch (ClassNotFoundException ex) {
      throw new IOException("Expected to find a serialized document here, " + "but found something else instead.", ex);
    }

    docInput.close();
    return document;
  }
  
  public static Document deserialize(DataInputStream stream, boolean compressed) throws IOException {
    ObjectInputStream docInput;
    if (compressed) {
      docInput = new ObjectInputStream(new GZIPInputStream(stream));
    } else {
      docInput = new ObjectInputStream(stream);
    }

    Document document = null;
    try {
      document = (Document) docInput.readObject();
    } catch (ClassNotFoundException ex) {
      throw new IOException("Expected to find a serialized document here, " + "but found something else instead.", ex);
    }

    docInput.close();
    return document;
  }
}
