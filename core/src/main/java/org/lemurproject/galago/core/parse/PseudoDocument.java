// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class PseudoDocument extends Document {

  public class Sample implements Serializable {

    public Sample(String src, int loc, String text) {
      source = src;
      location = loc;
      content = text;
    }
    public String source;
    public int location;
    public String content;
  }
  ArrayList<Sample> samples;

  public PseudoDocument() {
    super();
    samples = new ArrayList<Sample>();
  }

  public PseudoDocument(String externalIdentifier, List<String> text) {
    this();
    this.name = externalIdentifier;
    addSample("", 0, text);
  }

  public PseudoDocument(Document first) {
    this();
    identifier = first.identifier;
    name = first.name;
    metadata = first.metadata;
    terms = first.terms;
    tags = first.tags;
  }

  public void addSample(Document d) {
    if (samples.isEmpty()) {
      identifier = d.identifier;
      name = d.name;
      metadata = d.metadata;
      terms = d.terms;
      tags = d.tags;
    }
    terms.add("##");
    terms.addAll(d.terms);
    addSample(d.name, 0, d.terms);
  }

  public void addSample(String src, int loc, List<String> terms) {
    samples.add(new Sample(src, loc, Utility.join(terms.toArray(new String[0]))));
  }

  private void addSample(String src, int loc, String content) {
    samples.add(new Sample(src, loc, content));
  }

  @Override
  public String toString() {
    String start = super.toString();
    StringBuilder builder = new StringBuilder(start);
    builder.append("\n");
    for (Sample s : samples) {
      builder.append(String.format("SOURCE: %s\n, LOCATION: %s\n, CONTENTS: %s\n\n",
              s.source, s.location, s.content));
    }
    return builder.toString();
  }

  public static byte[] serialize(Parameters p, PseudoDocument doc) throws IOException {
    doc.text = null; // Even if there was text...too bad
    byte[] start = Document.serialize(p, doc);
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
    }
    ByteArrayOutputStream combinedBytes = new ByteArrayOutputStream();
    DataOutputStream combinedDataStream = new DataOutputStream(combinedBytes);
    System.err.printf("Writing sizes of %d and %d\n", start.length, sampleArray.size());
    combinedDataStream.writeInt(start.length);
    combinedDataStream.write(start);
    combinedDataStream.writeInt(sampleArray.size());
    combinedDataStream.write(sampleArray.toByteArray());
    return combinedBytes.toByteArray();
  }

  public static PseudoDocument deserialize(byte[] data, Parameters p) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(data);
    DataInputStream dataIStream = new DataInputStream(stream);
    return deserialize(dataIStream, p);
  }

  public static PseudoDocument deserialize(DataInputStream dataIStream, Parameters p) throws IOException {
    Parameters noText = p.clone();
    noText.set("text", false);

    // Read in the super data
    int superSize = dataIStream.readInt();
    byte[] superData = new byte[superSize];
    dataIStream.readFully(superData);
    Document d = Document.deserialize(superData, noText);
    PseudoDocument pd = new PseudoDocument(d);
    int samplesSize = 0;
    if (p.get("samples", true)) {
      samplesSize = dataIStream.readInt();
      byte[] sampleData = new byte[samplesSize];
      dataIStream.readFully(sampleData);
      ByteArrayInputStream sampleBytes = new ByteArrayInputStream(sampleData);
      DataInputStream sampleIStream = new DataInputStream(new SnappyInputStream(sampleBytes));
      int count = sampleIStream.readInt();
      byte[] buffer;
      for (int i = 0; i < count; ++i) {
        int len = sampleIStream.readInt();
        buffer = new byte[len];
        sampleIStream.readFully(buffer);
        String source = Utility.toString(buffer);
        int location = sampleIStream.readInt();
        len = sampleIStream.readInt();
        buffer = new byte[len];
        sampleIStream.readFully(buffer);
        String content = Utility.toString(buffer);
        pd.addSample(source, location, content);
      }
    }
    System.err.printf("Read PD(%d) |doc|=%d, |samples|=%d\n",
             d.identifier, superSize, samplesSize);
    return pd;
  }
}
