// BSD License (http://lemurproject.org)
package org.lemurproject.galago.tupleflow;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author trevor
 */
public class FileOrderedReader<T> implements ReaderSource<T> {

  InputStream dataStream;
  ArrayInput stream;
  TypeReader<T> orderedReader;
  String filename;
  Processor<T> processor;
  Order<T> order;
  private final CompressionType c;

  public FileOrderedReader(String filename) throws IOException {
    this(filename, 1024);
  }

  public FileOrderedReader(String filename, int bufferSize) throws IOException {
    // set up the input stream and get its length in bytes
    dataStream = StreamCreator.bufferedInputStream(filename);
    c = CompressionType.fromByte((byte) dataStream.read());

    // now, set up the stream, including a stopper that keeps us from
    // reading into the XML region (which no longer exists, but BufferedFileDataStream also buffers for us)
    switch (c) {
      case VBYTE:
        stream = new ArrayInput(new VByteInput(new DataInputStream(dataStream)));
      case GZIP:
        stream = new ArrayInput(new DataInputStream(new GZIPInputStream(dataStream)));
      case UNSPECIFIED:
      case NONE:
      default:
        stream = new ArrayInput(new DataInputStream(dataStream));
    }

    String className = stream.readString();
    String[] orderSpec = stream.readStrings();

    try {
      Class typeClass = Class.forName(className);
      org.lemurproject.galago.tupleflow.Type type = (org.lemurproject.galago.tupleflow.Type) typeClass.getConstructor().newInstance();
      order = type.getOrder(orderSpec);
    } catch (Exception e) {
      throw (IOException) new IOException(
              "Couldn't create an order object for type: " + className).initCause(e);
    }

    this.filename = filename;
    this.processor = null;
    this.orderedReader = order.orderedReader(stream, bufferSize);
  }

  @Override
  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    this.orderedReader.setProcessor(processor);
  }

  public Class<T> getOutputClass() {
    return order.getOrderedClass();
  }

  @Override
  public void run() throws IOException {
    orderedReader.run();
  }

  public Order<T> getOrder() {
    return order;
  }

  public TypeReader<T> getOrderedReader() {
    return orderedReader;
  }

  @Override
  public T read() throws IOException {
    T result = orderedReader.read();

    if (result == null) {
      close();
    }
    return result;
  }

  public void close() throws IOException {
    dataStream.close();
  }

  public CompressionType getCompression() {
    return c;
  }
}
