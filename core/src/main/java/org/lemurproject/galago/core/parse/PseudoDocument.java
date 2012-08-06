// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.*;
import java.util.ArrayList;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

public class PseudoDocument extends Document {
    public class Sample {
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

    public PseudoDocument(String externalIdentifier, String text) {
	this();
	this.name = externalIdentifier;
	addSample("", 0, text);
    }

    public PseudoDocument(Document first) {
	identifier = first.identifier;
	name = first.name;
	metadata = first.metadata;
	terms = first.terms;
	tags = first.tags;
    }

    public void addSample(Document d) {
	if (samples.size() == 0) {
	    identifier = d.identifier;
	    name = d.name;
	    metadata = d.metadata;
	    terms = d.terms;
	    tags = d.tags;	    
	}
	addSample(d.name, 0, d.text);
    }

    public void addSample(String src, int loc, String text) {
	samples.add(new Sample(src, loc, text));
    }

    public String toString() {
	String start = super.toString();
	StringBuilder builder = new StringBuilder(start);
	builder.append("\n");
	for(Sample s : samples) {
	    builder.append(String.format("SOURCE: %s\n, LOCATION: %s\n, CONTENTS: %s\n\n",
					 s.source, s.location, s.content));
	}
	return builder.toString();
    }

    public static byte[] serialize(Parameters p, PseudoDocument doc) throws IOException {
	doc.text = null; // Even if there was text...too bad
	byte[] start = Document.serialize(p, doc);
	ByteArrayOutputStream sampleArray = new ByteArrayOutputStream();
	DataOutputStream dataOStream = new DataOutputStream(sampleArray);
	dataOStream.write(start, 0, start.length);
	byte[] buffer;
	Utility.compressInt(dataOStream, doc.samples.size());
	for (Sample s : doc.samples) {
	    buffer = Utility.fromString(s.source);
	    Utility.compressInt(dataOStream, buffer.length);
	    dataOStream.write(buffer);
	    Utility.compressInt(dataOStream, s.location);
	    buffer = Utility.fromString(s.content);
	    Utility.compressInt(dataOStream, buffer.length);
	    dataOStream.write(buffer);
	}
	return sampleArray.toByteArray();
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
	Document d = Document.deserialize(dataIStream, noText);
	PseudoDocument pd = new PseudoDocument(d);
	int count = Utility.uncompressInt(dataIStream);
	byte[] buffer;
	for (int i = 0; i < count; ++i) {
	    int len = Utility.uncompressInt(dataIStream);
	    buffer = new byte[len];
	    dataIStream.readFully(buffer);
	    String source = Utility.toString(buffer);
	    int location = Utility.uncompressInt(dataIStream);
	    len = Utility.uncompressInt(dataIStream);
	    buffer = new byte[len];
	    dataIStream.readFully(buffer);
	    String content = Utility.toString(buffer);
	    pd.addSample(source, location, content);
	}
	return pd;
    }
}