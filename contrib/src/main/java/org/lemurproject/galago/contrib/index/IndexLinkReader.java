// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import ciir.proteus.galago.thrift.IndexLink;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;

/**
 * This is a dirty hack of a reader class. However it's not clear how
 * this data fits into the retrieval hierarchy, so for now we only provide
 * simple lookup functions. And interpretation of value bytes into 
 * constituent Parameter objects which are the encoding of the posting lists.
 *
 * @author irmarc
 */
public class IndexLinkReader {
    protected BTreeReader reader;
    protected BTreeReader.BTreeIterator iterator;
    TTransport transport;
    TCompactProtocol.Factory pFactory;

    public IndexLinkReader(String filename) throws FileNotFoundException, IOException {
	reader = BTreeFactory.getBTreeReader(filename);
	iterator = reader.getIterator();
	pFactory = new TCompactProtocol.Factory();
    }
    
    public IndexLinkReader(BTreeReader r) throws IOException {
	this.reader = r;
	iterator = reader.getIterator();
    }

    public Parameters getManifest() {
	return reader.getManifest();
    }

    public void close() throws IOException {
	reader.close();
    }

    public boolean containsKey(String id) throws IOException {
	byte[] key = Utility.fromString(id);
	iterator.find(key);
	return (Utility.compare(key, iterator.getKey()) == 0);
    }

    public IndexLink getLinks(String id) throws IOException {
	if (!containsKey(id)) {
	    return null;
	}
	try {
	    TTransport transport = 
		new TMemoryInputTransport(iterator.getValueBytes());
	    TProtocol protocol = pFactory.getProtocol(transport);
	    IndexLink link = new IndexLink();
	    link.read(protocol);
	    return link;
	} catch (TException te) {
	    throw new IOException(te);
	}
    }
}