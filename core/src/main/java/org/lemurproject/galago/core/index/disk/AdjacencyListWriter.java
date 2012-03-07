// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.CompressedRawByteBuffer;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.types.Adjacency;
import org.lemurproject.galago.core.util.DoubleCodec;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * Writes adjacencies, which are lists of <source, score> pairs.
 * 
 * @author irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.Adjacency", order = {"+source", "+destination"})
public class AdjacencyListWriter implements Adjacency.SourceDestinationOrder.ShreddedProcessor, Processor<Adjacency> {

  public class InvertedList implements IndexElement {

    CompressedRawByteBuffer data = new CompressedRawByteBuffer();
    CompressedByteBuffer header = new CompressedByteBuffer();
    int numNeighbors;
    int lastID;
    DoubleCodec codec;
    byte[] word;

    public InvertedList(byte[] word) {
      this.word = word;
      this.numNeighbors = 0;
      this.lastID = 0;
      codec = null;
    }

    public void write(final OutputStream stream) throws IOException {
      header.write(stream);
      header.clear();
      data.write(stream);
      data.clear();
    }

    public void addDestination(byte[] destination) throws IOException {
      int converted = Utility.toInt(destination);
      data.add(converted - lastID);
      lastID = converted;
      numNeighbors++;
    }

    public void addWeight(double weight) throws IOException {
      data.addDouble(weight);
    }

    public byte[] key() {
      return word;
    }

    public long dataLength() {
      return data.length() + header.length();
    }

    public void close() {
      header.add(numNeighbors);
      // For now the codec is never there - we need to implement compressed double writing
      header.addRaw((codec == null ? 0 : 1));
      if (codec != null) {
        header.add(Double.doubleToLongBits(codec.getSeed()));
      }
    }
  }
  DiskBTreeWriter writer;
  InvertedList list = null;

  /** Creates a new instance of AdjacencyListWriter */
  public AdjacencyListWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    writer = new DiskBTreeWriter(parameters);
    writer.getManifest().set("readerClass", AdjacencyListReader.class.getName());
    writer.getManifest().set("writerClass", getClass().getName());
  }

  public void processSource(byte[] source) throws IOException {
    if (list != null) {
      list.close();
      writer.add(list);
    }

    list = new InvertedList(source);
  }

  // SourceDestinationOrder.ShreddedProcessor
  public void processDestination(byte[] destination) throws IOException {
    list.addDestination(destination);
  }

  public void processTuple(double weight) throws IOException {
    list.addWeight(weight);
  }

  // SourceWeightOrder.ShreddedProcessor
  public void processWeight(double weight) throws IOException {
    list.addWeight(weight);
  }

  public void processTuple(byte[] destination) throws IOException {
    list.addDestination(destination);
  }
  // Generic Processor
  // just for this method
  byte[] lastSource = null;

  public void process(Adjacency object) throws IOException {
    if (lastSource == null
            || Utility.compare(lastSource, object.source) != 0) {
      if (list != null) {
        list.close();
        writer.add(list);
      }

      list = new InvertedList(object.source);
      lastSource = object.source;
    }
    list.addDestination(object.destination);
    list.addWeight(object.weight);
  }

  public void close() throws IOException {
    if (list != null) {
      list.close();
      writer.add(list);
    }
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("PositionIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }
}
