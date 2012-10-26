// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.index;

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
import org.lemurproject.galago.core.types.PictureOccurrence;

import ciir.proteus.galago.thrift.Coordinates;
import ciir.proteus.galago.thrift.Picture;
import ciir.proteus.galago.thrift.PictureList;

import org.lemurproject.galago.core.index.GenericElement;
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
@InputClass(className = "org.lemurproject.galago.core.types.PictureOccurrence",
	    order = {"+id", "+ordinal", "+top", "+left"})
public class PictureStoreWriter implements Processor<PictureOccurrence> {
    DiskBTreeWriter writer;
    Counter pictureCounter, listCounter;
    PictureList currentPictures;
    byte[] lastPrimaryKey;
    Parameters parameters;
    TTransport transport;
    TCompactProtocol.Factory protocolFactory;
    ByteArrayOutputStream byteStream;

    public PictureStoreWriter(TupleFlowParameters parameters) throws IOException {
	this.parameters = parameters.getJSON();
	pictureCounter = parameters.getCounter("Pictures written");
	listCounter = parameters.getCounter("Lists written");
	writer = null;
	currentPictures = null;
	byteStream = new ByteArrayOutputStream(32768);
	transport = new TIOStreamTransport(byteStream);
	protocolFactory = new TCompactProtocol.Factory(); 
	String dirpath = this.parameters.getString("filename");
	File f = new File(dirpath, "pictures.index");
	writer = new DiskBTreeWriter(f.getCanonicalPath(), this.parameters);
    }

    private void emit() throws IOException {
	if (currentPictures != null) {
	    try {
		TProtocol protocol = 
		    protocolFactory.getProtocol(transport);
		currentPictures.write(protocol);
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
	currentPictures = new PictureList();
    }

    public void process(PictureOccurrence picture) throws IOException {
	byte[] key = picture.id;
	if (lastPrimaryKey == null || 
	    Utility.compare(key, lastPrimaryKey) != 0) {
	    emit();
	    lastPrimaryKey = key;
	}
	Coordinates coords = new Coordinates();
	coords.top = picture.top;
	coords.bottom = picture.bottom;
	coords.left = picture.left;
	coords.right = picture.right;
	Picture p = new Picture();
	p.coordinates = coords;
	currentPictures.addToPictures(p);
	if (pictureCounter != null) {
	    pictureCounter.increment();
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
	    handler.addError("PictureStoreWriter requires a 'filename' parameter.");
	    return;
	}	
	String index = parameters.getJSON().getString("filename");
	Verification.requireWriteableDirectory(index, handler);
    }
}