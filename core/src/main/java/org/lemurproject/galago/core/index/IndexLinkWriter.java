// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import java.io.File;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.lemurproject.galago.core.types.IndexLink;

import ciir.proteus.galago.thrift.Target;

import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 *
 * @author irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.IndexLink",
	    order = {"+srctype", "+targettype", "+id", "+targetid", "+targetpos"})
public class IndexLinkWriter implements Processor<IndexLink> {
    DiskBTreeWriter writer;
    String from;
    String to;
    String filePrefix;
    Counter linkCounter, listCounter;
    ciir.proteus.galago.thrift.IndexLink postingList;
    byte[] lastPrimaryKey;
    Parameters parameters;
    TTransport transport;
    TCompactProtocol.Factory protocolFactory;
    ByteArrayOutputStream byteStream;
    Target currentTarget;
    Direction direction;

    // Describes src to target. As in, "src is IN target", or "src HAS target"
    enum Direction { IN, HAS };

    public IndexLinkWriter(TupleFlowParameters parameters) throws IOException {
	this.parameters = parameters.getJSON();
	filePrefix = this.parameters.getString("filename");
	linkCounter = parameters.getCounter("Links written");
	listCounter = parameters.getCounter("Lists written");
	writer = null;
	from = to = null;
	postingList = null;
	currentTarget = null;
	byteStream = new ByteArrayOutputStream(32768);
	transport = new TIOStreamTransport(byteStream);
	protocolFactory = new TCompactProtocol.Factory(); 
    }

    public void checkWriter(IndexLink link) throws IOException {
	if (!link.srctype.equals(from) ||
	    !link.targettype.equals(to)) {
	    if (writer != null) {
		emit();
		writer.close();
		lastPrimaryKey = null;
	    }
	    String filename = String.format("%s%s%s.%s",
					    filePrefix,
					    File.separator,
					    link.srctype,
					    link.targettype);
	    System.err.printf("Opening %s\n", filename);
	    writer = new DiskBTreeWriter(filename, this.parameters);
	    from = link.srctype;
	    to = link.targettype;
	    if (from.equals("collection") ||
		from.equals("page")) {
		direction = Direction.HAS;
	    } else {
		direction = Direction.IN;
	    }
	}
    }
    
    private void update(org.lemurproject.galago.core.types.IndexLink link) {
	if (currentTarget == null || 
	    !currentTarget.getId().equals(link.targetid) ||
	    !currentTarget.getType().equals(link.targettype)) {
	    currentTarget = new Target(link.targetid, link.targettype);
	    postingList.addToTarget(currentTarget);
	}
	switch (direction) {
	case IN: currentTarget.addToPositions(link.pos); break;
	case HAS: currentTarget.addToPositions(link.targetpos); break;
	}
    }

    private void emit() throws IOException {
	if (postingList != null) {
	    if (postingList.isSetTarget()) {
		try {
		    TProtocol protocol = 
			protocolFactory.getProtocol(transport);
		    postingList.write(protocol);
		    GenericElement indexElement = 
			new GenericElement(lastPrimaryKey, 
					   byteStream.toByteArray());
		    writer.add(indexElement);
		    byteStream.reset();
		    if (listCounter != null) {
			listCounter.increment();
		    }
		} catch (TException te) {
		    throw new IOException(te);
		}
	    }
	}
	postingList = new ciir.proteus.galago.thrift.IndexLink();
    }

    public void process(IndexLink link) throws IOException {
	checkWriter(link);
	byte[] key = link.id;
	if (lastPrimaryKey == null || 
	    Utility.compare(key, lastPrimaryKey) != 0) {
	    emit();
	    postingList.setSrcid(Utility.toString(key));
	    postingList.setSrctype(link.srctype);
	    lastPrimaryKey = key;
	}
	update(link);
	if (linkCounter != null) {
	    linkCounter.increment();
	}
    }

    public void close() throws IOException {
	if (writer != null) {
	    emit();
	    writer.close();
	}
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
	if (!parameters.getJSON().isString("filename")) {
	    handler.addError("IndexLinkWriter requires a 'filename' parameter.");
	    return;
	}	
	String index = parameters.getJSON().getString("filename");
	Verification.requireWriteableDirectory(index, handler);
    }
}