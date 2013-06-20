// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import org.lemurproject.galago.core.types.WordDateCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
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
	int entryCount;

	public InvertedList(byte[] key) {
	    this.key = key;
	    data = new CompressedRawByteBuffer();
	    danglingCount = 0;
	    entryCount = 0;
	}

	public byte[] key() { 
	    return key; 
	}

	public long dataLength() {
	    return data.length() + 4;
	}
	
	public void write(final OutputStream oStream) throws IOException {
	    oStream.write(Utility.fromInt(entryCount));
	    data.write(oStream);
	    data.clear();
	}
	
	public void addDate(int date) throws IOException {
	    if (danglingCount != 0) {
		data.add(danglingCount);
	    }
	    data.add((long)date);	    
	    danglingCount = 0;
	    ++entryCount;
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

    public static void verify(TupleFlowParameters parameters, ErrorStore store) {
	if (!parameters.getJSON().isString("filename")) {
	    store.addError("PictureStoreWriter requires a 'filename' parameter.");
	    return;
	}	
	String index = parameters.getJSON().getString("filename");
	Verification.requireWriteableFile(index, store);
    }
}