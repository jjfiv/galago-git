// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.lemurproject.galago.core.corpus.DocumentSerializer;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Document implements Serializable {

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

  @Deprecated
  public static byte[] serialize(Document doc, Parameters conf) throws IOException {
    return DocumentSerializer.instance(conf).toBytes(doc);
  }

  @Deprecated
  public static Document deserialize(byte[] data, Parameters manifest, DocumentComponents selection) throws IOException {
    return DocumentSerializer.instance(manifest).fromBytes(data, selection);
  }

  @Deprecated
  public static Document deserialize(DataInputStream stream, Parameters manifest, DocumentComponents selection) throws IOException {
    return DocumentSerializer.instance(manifest).fromStream(stream, selection);
  }

  public TObjectIntHashMap<String> getBagOfWords() {
    TObjectIntHashMap<String> termCounts = new TObjectIntHashMap<String>();
    for(String term : terms) {
      termCounts.adjustOrPutValue(term, 1, 1);
    }
    return termCounts;
  }

  /**
   * This class allows the selection of parts of the document to serialize or deserialize.
   */
  public static class DocumentComponents implements Serializable {
    public static DocumentComponents All = new DocumentComponents(true, true, true);
    public static DocumentComponents JustMetadata = new DocumentComponents(false, true, false);
    public static DocumentComponents JustText = new DocumentComponents(true, false, false);
    public static DocumentComponents JustTerms = new DocumentComponents(true, false, true);

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

