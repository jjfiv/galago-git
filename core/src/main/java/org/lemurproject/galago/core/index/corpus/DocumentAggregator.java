// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.PseudoDocument;
import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;

@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})
public class DocumentAggregator implements KeyValuePair.KeyOrder.ShreddedProcessor {
    Counter docsIn, docsOut;
    DiskBTreeWriter writer;
    int documentNumber = 0;    
    byte[] lastIdentifier = null;
    Map<String, PseudoDocument> bufferedDocuments;

    public DocumentAggregator(TupleFlowParameters parameters) throws IOException, FileNotFoundException {
	docsIn = parameters.getCounter("Documents in");
	docsOut = parameters.getCounter("Documents out");
	writer = new DiskBTreeWriter(parameters);
	bufferedDocuments = new HashMap<String, PseudoDocument>();
    }

    public void processKey(byte[] key) throws IOException {
	if (lastIdentifier == null || 
	    Utility.compare(key, lastIdentifier) != 0) {
	    if (lastIdentifier != null) {
		write();
	    }
	    lastIdentifier = key;
	}
	if (docsIn != null) docsIn.increment();
    }

    public void processTuple(byte[] value) throws IOException {
	ByteArrayInputStream stream = new ByteArrayInputStream(value);
	Document document;
	try {
	    ObjectInputStream input = new ObjectInputStream(stream);
	    document = (Document) input.readObject();
	    addToBuffer(document);
	} catch (ClassNotFoundException cnfe) {
	    throw new RuntimeException(cnfe);
	}
    }

    private void addToBuffer(Document d) {
	if (!bufferedDocuments.containsKey(d.name)) {
	    bufferedDocuments.put(d.name, new PseudoDocument(d));
	} else {
	    bufferedDocuments.get(d.name).addSample(d);
	}
    }

    private void write() throws IOException {
	for (String nameKey : bufferedDocuments.keySet()) {
	    ByteArrayOutputStream array = new ByteArrayOutputStream();
	    ObjectOutputStream output = new ObjectOutputStream(array);
	    output.writeObject(bufferedDocuments.get(nameKey));
	    output.close();
	    byte[] newKey = Utility.fromInt(documentNumber++);
	    byte[] value = array.toByteArray();
	    writer.add(new GenericElement(newKey, value));
	    if (docsOut != null) docsOut.increment();
	}
	bufferedDocuments.clear();
    }

    public void close() throws IOException {
	write();
	writer.close();
    }
}
