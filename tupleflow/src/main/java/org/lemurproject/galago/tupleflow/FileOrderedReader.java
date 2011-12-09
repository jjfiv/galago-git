// BSD License (http://lemurproject.org)

package org.lemurproject.galago.tupleflow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author trevor
 */
public class FileOrderedReader<T> implements ReaderSource<T> {
    RandomAccessFile dataStream;
    ArrayInput stream;
    TypeReader<T> orderedReader;
    String filename;
    Processor<T> processor;
    Order<T> order;

    public FileOrderedReader(String filename, int bufferSize, boolean compressed) throws IOException {
        // set up the input stream and get its length in bytes
        dataStream = StreamCreator.inputStream(filename);
        long fileLength = dataStream.length();

        // now, set up the stream, including a stopper that keeps us from
        // reading into the XML region (which no longer exists, but BufferedFileDataStream also buffers for us)
        if (compressed) {
            stream = new ArrayInput(new VByteInput(new BufferedFileDataStream(dataStream, fileLength)));
        } else {
            stream = new ArrayInput(new BufferedFileDataStream(dataStream, fileLength));
        }

        String className = stream.readString();
        String[] orderSpec = stream.readStrings();

        try {
            Class typeClass = Class.forName(className);
            org.lemurproject.galago.tupleflow.Type type = (org.lemurproject.galago.tupleflow.Type) typeClass.
                    getConstructor().newInstance();
            order = type.getOrder(orderSpec);
        } catch (Exception e) {
            throw (IOException) new IOException(
                    "Couldn't create an order object for type: " + className).initCause(e);
        }

        this.filename = filename;
        this.processor = null;
        this.orderedReader = order.orderedReader(stream, bufferSize);
    }

    public FileOrderedReader(String filename) throws IOException {
        this(filename, 1024, true);
    }

    /** Creates a new instance of FileOrderedReader */
    public FileOrderedReader(String filename, Order<T> order, int bufferSize, boolean compressed) throws IOException {
        this(filename, bufferSize, compressed);

        if (order.getOrderedClass() != this.order.getOrderedClass()) {
            throw (IOException) new IOException("This file, '" + filename + "', contains objects of type " +
                                                this.order.getOrderedClass() + "' even though objects of type " +
                                                order.getOrderedClass() + "' were expected.");
        }
    }

    public FileOrderedReader(String filename, Order<T> order, int bufferSize) throws IOException {
        this(filename, order, bufferSize, true);
    }

    public FileOrderedReader(String filename, Order<T> order) throws IOException {
        this(filename, order, 1024, true);
    }

    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        this.orderedReader.setProcessor(processor);
    }

    public Class<T> getOutputClass() {
        return order.getOrderedClass();
    }

    public void run() throws IOException {
        orderedReader.run();
    }

    public Order<T> getOrder() {
        return order;
    }

    public TypeReader<T> getOrderedReader() {
        return orderedReader;
    }

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
}
