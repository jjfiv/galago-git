/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key", "+value"})
public class ConflationIndexWriter implements KeyValuePair.KeyValueOrder.ShreddedProcessor {

  private DiskBTreeWriter writer;
  private ConflationList current = null;

  public ConflationIndexWriter(TupleFlowParameters parameters) throws IOException {
    writer = new DiskBTreeWriter(parameters);
    writer.getManifest().set("readerClass", ConflationIndexReader.class.getName());
    writer.getManifest().set("writerClass", getClass().getName());
  }

  @Override
  public void processKey(byte[] key) throws IOException {
    if(current != null){
      writer.add(current);
    }
    current = new ConflationList(key);
  }

  @Override
  public void processValue(byte[] value) throws IOException {
    current.addValue(value);
  }

  @Override
  public void processTuple() throws IOException {
    // pass - this will discard repeated values //
  }


  @Override
  public void close() throws IOException {
    if(current != null){
      writer.add(current);
    }
    writer.close();
  }

  
  
  private static class ConflationList implements IndexElement {

    byte[] key;
    int valueCount;
    ByteArrayOutputStream valueBuffer;
    DataOutputStream valueStream;

    public ConflationList(byte[] key) {
      this.key = key;
      this.valueCount = 0;
      this.valueBuffer = new ByteArrayOutputStream();
      this.valueStream = new DataOutputStream(valueBuffer);
    }

    public void addValue(byte[] value) throws IOException {
      valueCount++;
      valueStream.writeInt(value.length);
      valueStream.write(value);
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public long dataLength() {
      return 4 // string count
              + valueBuffer.size(); //value data
    }

    @Override
    public void write(OutputStream stream) throws IOException {
      stream.write(Utility.fromInt(valueCount));
      valueStream.close();
      stream.write( valueBuffer.toByteArray() );
    }
  }
}
