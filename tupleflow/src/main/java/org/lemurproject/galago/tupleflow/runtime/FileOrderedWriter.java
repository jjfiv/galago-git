// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.runtime;

import org.lemurproject.galago.tupleflow.ArrayOutput;
import org.lemurproject.galago.tupleflow.CompressionType;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.utility.StreamCreator;
import org.lemurproject.galago.utility.buffer.VByteOutput;

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
    ArrayOutput stream;
    Processor<T> orderedWriter;

    public FileOrderedWriter(String filename, Order<T> order, CompressionType c) throws IOException {
        this.filename = filename;
        this.order = order;

        DataOutputStream dataStream = StreamCreator.realOutputStream(filename);

        switch(c){
            // well-specified.
            case NONE:
            case VBYTE:
            case GZIP:
                break;

            case UNSPECIFIED:
            default:
                // UNSPECIFIED and DEFAULT are all the same -- choose GZIP:
                c = CompressionType.GZIP;
                break;
        }

        // write the compression type (un compressed)
        dataStream.writeByte(CompressionType.toByte(c));
        switch(c){
            case VBYTE:
                stream = new ArrayOutput(new VByteOutput(new DataOutputStream(new GZIPOutputStream(dataStream))));
                break;
            case GZIP:
                stream = new ArrayOutput(new DataOutputStream(new GZIPOutputStream(dataStream)));
                break;
            case NONE:
                stream = new ArrayOutput(dataStream);
                break;
            default:
                throw new RuntimeException("Compression Logic in FileOrderedWriter broken!");
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
        stream.close();
    }
}
