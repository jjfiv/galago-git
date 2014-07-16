// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.utility.StreamCreator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author trevor
 */
public class FileOrderedWriter<T> implements Processor<T> {
    String filename;
    Order<T> order;
    DataOutputStream dataStream;
    ArrayOutput stream;
    Processor<T> orderedWriter;

    public FileOrderedWriter(String filename, Order<T> order, CompressionType c) throws IOException {
        this.filename = filename;
        this.order = order;

        dataStream = StreamCreator.realOutputStream(filename);
        // write the compression type (un compressed)
        dataStream.writeByte(CompressionType.toByte(c));
        
        switch(c){
          case VBYTE:
            stream = new ArrayOutput(new VByteOutput(dataStream));
            break;
          case GZIP:
            // need to be able to call close on GZIP stream.
            dataStream = new DataOutputStream(new GZIPOutputStream(dataStream));
            stream = new ArrayOutput(dataStream);
            break;

            // UNSPECIFIED, DEFAULT, NONE are all the same -- no compression
          case UNSPECIFIED:
          case NONE:
          default:
            stream = new ArrayOutput(dataStream);
            break;
        }
        stream.writeString(order.getOrderedClass().getName());
        stream.writeStrings(order.getOrderSpec());
        this.orderedWriter = order.orderedWriter(stream);
    }

    public Class<T> getInputClass() {
        return order.getOrderedClass();
    }

  @Override
    public void process(T object) throws IOException {
        orderedWriter.process(object);
    }

  @Override
    public void close() throws IOException {
        orderedWriter.close(); // this function flushes an internal buffers
        dataStream.close(); // this function flushes the file buffers
    }
}
