// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.KeyValueWriter;
import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import org.lemurproject.galago.core.types.WordDateCount;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Sorter;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/** 
 * @author irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.WordDateCount",
	    order = {"+word", "+date"})
public class WordDateCountWriter implements WordDateCount.WordDateOrder.ShreddedProcessor {
    public class InvertedList implements IndexElement {
	byte[] key;
	CompressedRawByteBuffer data;
	int danglingCount;

	public InvertedList(byte[] key) {
	    this.key = key;
	    data = new CompressedRawByteBuffer();
	    danglingCount = 0;
	}

	public byte[] key() { 
	    return key; 
	}

	public long dataLength() {
	    return data.length();
	}
	
	public void write(final OutputStream oStream) throws IOException {
	    data.write(oStream);
	    data.clear();
	}
	
	public void addDate(int date) throws IOException {
	    if (danglingCount != 0) {
		data.add(danglingCount);
	    }
	    data.add((long)date);	    
	    danglingCount = 0;
	}

	public void incrementCount(long count) throws IOException {
	    danglingCount += count;
	}

	public void close() throws IOException {
	    data.add(danglingCount);
	}
    }

    DiskBTreeWriter writer;
    InvertedList invertedList;
    Parameters actualParams;
    public WordDateCountWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
	actualParams = parameters.getJSON();
	actualParams.set("writerClass", getClass().getName());
	writer = new DiskBTreeWriter(parameters);
    }

    @Override
    public void processWord(byte[] wordBytes) throws IOException {
	if (invertedList != null) {
	    invertedList.close();
	    writer.add(invertedList);
	}
	invertedList = new InvertedList(wordBytes);
    }

    @Override
    public void processDate(int date) throws IOException {
	invertedList.addDate(date);
    }
    
    @Override
    public void processTuple(long count) throws IOException {
	invertedList.incrementCount(count);
    }

    @Override
    public void close() throws IOException {
	if (invertedList != null) {
	    invertedList.close();
	}
	writer.add(invertedList);
	writer.close();
    }

    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
	if (!parameters.getJSON().isString("filename")) {
	    handler.addError("PictureStoreWriter requires a 'filename' parameter.");
	    return;
	}	
	String index = parameters.getJSON().getString("filename");
	Verification.requireWriteableFile(index, handler);
    }
}