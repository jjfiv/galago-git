// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.DiskSpillCompressedByteBuffer;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.types.NumberWordProbability;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author trevor
 */
public class SparseFloatListWriter implements
        NumberWordProbability.NumberWordOrder.ShreddedProcessor {

  DiskBTreeWriter writer;
  DoubleInvertedList list;

  /**
   * Creates a new instance of DoubleListWriter
   */
  public SparseFloatListWriter(TupleFlowParameters parameters) throws IOException {
    writer = new DiskBTreeWriter(parameters);
    writer.getManifest().set("readerClass", SparseFloatListReader.class.getName());
    writer.getManifest().set("writerClass", getClass().getName());
  }

  @Override
  public void processWord(byte[] word) throws IOException {
    if (list != null) {
      list.close();
      writer.add(list);
    }

    list = new DoubleInvertedList(word);
  }

  @Override
  public void processNumber(long number) throws IOException {
    list.addDocument(number);
  }

  @Override
  public void processTuple(double probability) throws IOException {
    list.addProbability(probability);
  }

  @Override
  public void close() throws IOException {
    if (list != null) {
      list.close();
      writer.add(list);
    }

    writer.close();
  }

  public static class DoubleInvertedList implements IndexElement {

    DiskSpillCompressedByteBuffer data = new DiskSpillCompressedByteBuffer();
    CompressedByteBuffer header = new CompressedByteBuffer();
    long lastDocument;
    long documentCount;
    byte[] word;

    public DoubleInvertedList(byte[] word) {
      this.word = word;
      this.lastDocument = 0;
      this.documentCount = 0;
    }

    @Override
    public void write(final OutputStream stream) throws IOException {
      header.write(stream);
      header.clear();
      data.write(stream);
      data.clear();
    }

    public void addDocument(long document) throws IOException {
      data.add(document - lastDocument);
      documentCount++;
      lastDocument = document;
    }

    public void addProbability(double probability) throws IOException {
      data.addFloat((float) probability);
    }

    @Override
    public byte[] key() {
      return word;
    }

    @Override
    public long dataLength() {
      return data.length() + header.length();
    }

    public void close() {
      header.add(documentCount);
    }
  }
}
