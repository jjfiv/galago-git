// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Document implements Serializable {

  static final int BUFFER_SIZE = 8192;
  // document id - this value is serialized
  public long identifier = -1;
  // document data - these values are serialized
  public String name;
  public Map<String, String> metadata;
  public String text;
  public List<String> terms;
  public List<Integer> termCharBegin = new ArrayList<Integer>();
  public List<Integer> termCharEnd = new ArrayList<Integer>();
  public List<Tag> tags;
  // other data - used to generate an identifier; these values can not be serialized!
  public int fileId = -1;
  public int totalFileCount = -1;
  public String filePath = "";
  public long fileLocation = -1;

  public Document() {
    metadata = new HashMap<String,String>();
  }

  public Document(String externalIdentifier, String text) {
    this();
    this.name = externalIdentifier;
    this.text = text;
  }

  public Document(Document d) {
    this.identifier = d.identifier;
    this.name = d.name;
    this.metadata = new HashMap<String,String>(d.metadata);
    this.text = d.text;
    this.terms = new ArrayList<String>(d.terms);
    this.termCharBegin = new ArrayList<Integer>(d.termCharBegin);
    this.termCharEnd = new ArrayList<Integer>(d.termCharEnd);
    this.tags = new ArrayList<Tag>(d.tags);
    this.fileId = d.fileId;
    this.totalFileCount = d.totalFileCount;
  }

  @Override
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
      int count = 0;
      sb.append("\nTags: \n");
      for (Tag t : tags) {
        sb.append(count).append(" : ");
        sb.append(t.toString()).append("\n");
        count += 1;
      }
    }

    if (terms != null) {
      int count = 0;
      sb.append("\nTerm vector: \n");
      for (String s : terms) {
        sb.append(count).append(" : ");
        sb.append(s).append("\n");
        count += 1;
      }
    }

    if (text != null) {
      sb.append("\nText :").append(text);
    }
    sb.append("\n");

    return sb.toString();
  }

  public static byte[] serialize(Document doc, Parameters conf) throws IOException {
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

//  [sjh] : Deprecated because it requires TOO much space for large documents,
//          Use TagTokenizer to regenerate
//          -- you can get the list of indexed fields from the corpus parameters.

//    ByteArrayOutputStream tagsArray = new ByteArrayOutputStream();
//    output = new DataOutputStream(tagsArray);
//    // tags
//    if (doc.tags == null) {
//      output.writeInt(0);
//    } else {
//      output.writeInt(doc.tags.size());
//      for (Tag tag : doc.tags) {
//        bytes = Utility.fromString(tag.name);
//        output.writeInt(bytes.length);
//        output.write(bytes);
//        output.writeInt(tag.begin);
//        output.writeInt(tag.end);
//        output.writeInt(tag.attributes.size());
//        for (String key : tag.attributes.keySet()) {
//          bytes = Utility.fromString(key);
//          output.writeInt(bytes.length);
//          output.write(bytes);
//          bytes = Utility.fromString(tag.attributes.get(key));
//          output.writeInt(bytes.length);
//          output.write(bytes);
//        }
//      }
//    }
//    output.close();
//
//
//    ByteArrayOutputStream termsArray = new ByteArrayOutputStream();
//    output = new DataOutputStream(termsArray);
//    DataOutput vOutput = new VByteOutput(output);
//
//    // terms
//    if (doc.terms == null) {
//      vOutput.writeInt(0);
//    } else {
//      vOutput.writeInt(doc.terms.size());
//      int begin = 0;
//      int end = 0;
//      for(int i=0; i<doc.termCharBegin.size();i++){
//        // begin - prevEnd // -- d-gapping
//        begin = doc.termCharBegin.get(i) - end;
//        end = doc.termCharEnd.get(i) - begin;
//        assert(begin >= 0);
//        assert(end >= 0);
//        vOutput.writeInt(begin);
//        vOutput.writeInt(end);
//      }
//    }
//    output.close();

    ByteArrayOutputStream docArray = new ByteArrayOutputStream();
    output = new DataOutputStream(new SnappyOutputStream(docArray));

    output.writeInt(metadataArray.size());
    output.writeInt(textArray.size());
//    output.writeInt(tagsArray.size());
//    output.writeInt(termsArray.size());

    output.write(headerArray.toByteArray());
    output.write(metadataArray.toByteArray());
    output.write(textArray.toByteArray());
//    output.write(tagsArray.toByteArray());
//    output.write(termsArray.toByteArray());

    output.close();

    return docArray.toByteArray();
  }

  public static Document deserialize(byte[] data, Parameters manifest, DocumentComponents selection) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    return deserialize(new DataInputStream(stream), manifest, selection);
  }

  public static Document deserialize(DataInputStream stream, Parameters manifest, DocumentComponents selection) throws IOException {
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
        buffer = sizeCheck(buffer, blen);
        input.readFully(buffer, 0, blen);
        String value = Utility.toString(buffer, 0, blen);

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
    return d;
  }

  protected static byte[] sizeCheck(byte[] currentBuffer, int sz) {
    if (sz > currentBuffer.length) {
      return new byte[sz];
    } else {
      return currentBuffer;
    }
  }

  /**
   * This class allows the selection
   *
   */
  public static class DocumentComponents implements Serializable {

    public boolean text = true;
    public boolean metadata = true;
    public boolean tokenize = false;
    // these variables can be used to restrict the text to just a short section at the start of the document
    // useful for massive files
    // start and end are byte offsets
    // -1 indicates no restriction
    public int subTextStart = -1;
    public int subTextLen = -1;

    // defaults
    public DocumentComponents() {
    }

    public DocumentComponents(boolean text, boolean metadata, boolean tokenize) {
      this.text = text;
      this.metadata = metadata;
      this.tokenize = tokenize;

      validate();
    }

    public DocumentComponents(Parameters p) {
      this.text = p.get("text", text);
      this.metadata = p.get("metadata", metadata);
      this.tokenize = p.get("tokenize", tokenize);

      validate();
    }

    /**
     * Because I prefer an illegal argument exception earlier to a null pointer exception in the tokenizer code, don't you?
     */
    public void validate() {
      if(!text && tokenize) {
        throw new IllegalArgumentException("DocumentComponents doesn't make sense! Found tokenize=true with text=false.");
      }
    }

    public Parameters toJSON() {
      Parameters p = new Parameters();
      p.put("text", text);
      p.put("metadata", metadata);
      p.put("tokenize", tokenize);
      return p;
    }

    @Override
    public String toString() {
      return toJSON().toString();
    }
  }
}

