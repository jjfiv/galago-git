// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.core.index.BTreeWriter;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.StreamCreator;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 *
 * Split index value writer
 *  - Index is a mapping from byte[] to byte[]
 *
 *  - allows values to be written out of order to a set of files
 *  - a unified ordered key structure should be kept in a folder 
 *    with these value files, as created by SplitIndexKeyWriter
 *  - SplitIndexReader will read this data
 * 
 *  This class if useful for writing a corpus structure
 *  - documents can be written to disk in any order
 *  - the key structure allows the documents to be found quickly
 *  - class is more efficient if the
 *    documents are inserted in sorted order
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class SplitBTreeValueWriter extends BTreeWriter
        implements KeyValuePair.KeyValueOrder.ShreddedProcessor {

  public static final long MAGIC_NUMBER = 0x2b3c4d5e6f7a8b9cL;
  public Processor<KeyValuePair> processor;
  public Parameters manifest;
  private int valueOutputId;
  private DataOutputStream valueOutput;
  private byte[] lastKey = null;
  private ByteArrayOutputStream keyArray;
  private DataOutputStream keyStream;
  private long valueOffset;
  private long valueLength;
  private short valueBlockSize;
  private Counter docCounter;

  public SplitBTreeValueWriter(TupleFlowParameters parameters) throws IOException {
    String valueOutputPath = parameters.getJSON().getString("filename") + File.separator + parameters.getInstanceId();
    Utility.makeParentDirectories(valueOutputPath);
    docCounter = parameters.getCounter("Document Values Stored");
    manifest = parameters.getJSON();

    valueOutputId = parameters.getInstanceId();
    valueOutput = StreamCreator.realOutputStream(valueOutputPath);
    valueOffset = 0;

    if (parameters.getJSON().get("blockIndex", false)) {
      valueBlockSize = (short) parameters.getJSON().get("valueBlockSize", 32768);
    } else {
      valueBlockSize = 0;
    }
  }

  @Override
  public Parameters getManifest() {
    return manifest;
  }

  @Override
  public void add(IndexElement list) throws IOException {
    processKey(list.key());
    valueOffset += list.dataLength();
    valueLength += list.dataLength();
    list.write(valueOutput);
  }

  @Override
  public long getValueBlockSize() {
    return manifest.get("valueBlockSize", valueBlockSize);
  }

  @Override
  public void processKey(byte[] key) throws IOException {

    if (lastKey != null) {
      keyStream.writeLong(valueLength); // value length
      keyStream.close();
      processor.process(new KeyValuePair(lastKey, keyArray.toByteArray()));
      if (docCounter != null) {
        docCounter.increment();
      }
    }

    lastKey = key;
    keyArray = new ByteArrayOutputStream();
    keyStream = new DataOutputStream(keyArray);

    keyStream.writeInt(valueOutputId); // file
    keyStream.writeLong(valueOffset); //valueOffset

    valueLength = 0;
  }

  /**
   * TODO: This needs to be changed to use blocks properly
   *  - currently it's identical to the add function above
   */
  @Override
  public void processValue(byte[] value) throws IOException {
    valueOutput.write(value);
    valueLength += value.length;
    valueOffset += value.length;
  }

  public void processTuple() throws IOException {
    // nothing //
  }

  public void close() throws IOException {
    if (lastKey != null) {
      keyStream.writeLong(valueLength); // value length
      keyStream.close();
      processor.process(new KeyValuePair(lastKey, keyArray.toByteArray()));
    }

    // write the value block size
    valueOutput.writeShort(valueBlockSize);
    // write the magic number.
    valueOutput.writeLong(MAGIC_NUMBER);
    valueOutput.close();
    processor.close();
  }

  @Override
  public void setProcessor(Step next) throws IncompatibleProcessorException {
    Linkage.link(this, next);
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("DocumentIndexWriter requires an 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableDirectory(index, handler);
  }
}
