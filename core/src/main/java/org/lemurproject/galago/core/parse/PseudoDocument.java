// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.*;
import java.util.ArrayList;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class PseudoDocument extends Document {

  public class Sample implements Serializable {

    public Sample(String s, int l, String c, String e) {
      source = s;
      location = l;
      content = c;
      externalLink = e;
    }

    public Sample(Document d) {
      source = String.format("%s_%s",
              d.metadata.get("sourceIdentifier"),
              d.metadata.get("startPage"));
      location = Integer.parseInt(d.metadata.get("startPos"));
      if (d.metadata.containsKey("externalLink")) {
        externalLink = d.metadata.get("externalLink");
      } else {
        externalLink = null;
      }
      content = Utility.join(d.terms.toArray(new String[0]), " ");
    }
    public String source;
    public int location;
    public String content;
    public String externalLink;
  }
  public ArrayList<Sample> samples;

  public PseudoDocument() {
    super();
    samples = new ArrayList<Sample>();
  }

  public PseudoDocument(Document first) {
    this();
    addSample(first);
  }

  public void addSample(Document d) {
    if (samples.isEmpty()) {
      identifier = d.identifier;
      name = d.name;
      metadata.putAll(d.metadata);
      metadata.remove("sourceIndentifier");
      metadata.remove("startPage");
      metadata.remove("startPos");
      terms = new ArrayList<String>();
    }
    terms.addAll(d.terms);
    terms.add("##");
    samples.add(new Sample(d));
  }

  private void addSample(String src, int loc, String content, String extLink) {
    samples.add(new Sample(src, loc, content, extLink));
  }

  @Override
  public String toString() {
    String start = super.toString();
    StringBuilder builder = new StringBuilder(start);
    builder.append("\n");
    for (Sample s : samples) {
      builder.append(String.format("SOURCE: %s\n LOCATION: %s\n CONTENTS: %s\n",
              s.source, s.location, s.content));
      if (s.externalLink != null) {
        builder.append(String.format("EXTERNAL LINK: %s\n", s.externalLink));
      }
    }
    builder.append("\n");
    return builder.toString();
  }

  public static byte[] serialize(Parameters p, PseudoDocument doc) throws IOException {
    doc.text = null; // Even if there was text...too bad
    byte[] start = Document.serialize(doc);
    ByteArrayOutputStream sampleArray = new ByteArrayOutputStream();
    DataOutputStream dataOStream = new DataOutputStream(new SnappyOutputStream(sampleArray));
    dataOStream.writeInt(doc.samples.size());
    byte[] buffer;
    for (Sample s : doc.samples) {
      buffer = Utility.fromString(s.source);
      dataOStream.writeInt(buffer.length);
      dataOStream.write(buffer);
      dataOStream.writeInt(s.location);
      buffer = Utility.fromString(s.content);
      dataOStream.writeInt(buffer.length);
      dataOStream.write(buffer);
      if (s.externalLink != null) {
        buffer = Utility.fromString(s.externalLink);
        dataOStream.writeInt(buffer.length);
        dataOStream.write(buffer);
      } else {
        dataOStream.writeInt(0);
      }
    }
    dataOStream.close();
    ByteArrayOutputStream combinedBytes = new ByteArrayOutputStream();
    DataOutputStream combinedDataStream = new DataOutputStream(combinedBytes);
    combinedDataStream.writeInt(start.length);
    combinedDataStream.write(start);
    combinedDataStream.writeInt(sampleArray.size());
    combinedDataStream.write(sampleArray.toByteArray());
    combinedDataStream.close();
    return combinedBytes.toByteArray();
  }

  public static PseudoDocument deserialize(byte[] data, PsuedoDocumentComponents s) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    DataInputStream dataIStream = new DataInputStream(stream);
    return deserialize(dataIStream, s);
  }

  public static PseudoDocument deserialize(DataInputStream dataIStream, PsuedoDocumentComponents s) throws IOException {
    s.text = false;

    // Read in the super data
    int superSize = dataIStream.readInt();
    byte[] superData = new byte[superSize];
    dataIStream.readFully(superData);
    Document d = Document.deserialize(superData, s);
    PseudoDocument pd = new PseudoDocument();
    pd.identifier = d.identifier;
    pd.name = d.name;
    pd.metadata = d.metadata;
    pd.text = d.text;
    pd.terms = d.terms;
    pd.tags = d.tags;
    int samplesSize = 0;
    if (s.samples) {
      samplesSize = dataIStream.readInt();
      System.err.printf("Size of sample data: %d\n", samplesSize);
      byte[] sampleData = new byte[samplesSize];
      dataIStream.readFully(sampleData);
      ByteArrayInputStream sampleBytes = new ByteArrayInputStream(sampleData);
      DataInputStream sampleIStream = new DataInputStream(new SnappyInputStream(sampleBytes));
      int count = sampleIStream.readInt();
      int sampleLimit = (int) s.sampleLimit;
      System.err.printf("Number of samples: %d, limit: %d\n", count, sampleLimit);
      int stop = Math.min(count, sampleLimit);
      byte[] buffer;
      for (int i = 0; i < stop; ++i) {
        int len = sampleIStream.readInt();
        buffer = new byte[len];
        sampleIStream.readFully(buffer);
        String source = Utility.toString(buffer);
        int location = sampleIStream.readInt();
        len = sampleIStream.readInt();
        buffer = new byte[len];
        sampleIStream.readFully(buffer);
        String content = Utility.toString(buffer);
        len = sampleIStream.readInt();
        String externalLink = null;
        if (len > 0) {
          buffer = new byte[len];
          sampleIStream.readFully(buffer);
          externalLink = Utility.toString(buffer);
        }
        pd.addSample(source, location, content, externalLink);
      }
    }
    return pd;
  }

  public static class PsuedoDocumentComponents extends DocumentComponents {
    public boolean samples = true;
    public int sampleLimit = Integer.MAX_VALUE;
    
    public PsuedoDocumentComponents(boolean text, boolean terms, boolean tags, boolean metadata, boolean samples) {
      super(text, terms, tags, metadata);
      this.samples = samples;
    }
  }
}
