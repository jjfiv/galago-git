// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

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

    public FileOrderedWriter(String filename, Order<T> order, boolean compressed) throws IOException {
        this.filename = filename;
        this.order = order;

        dataStream = StreamCreator.realOutputStream(filename);
        if (compressed) {
            stream = new ArrayOutput(new VByteOutput(dataStream));
        } else {
            stream = new ArrayOutput(dataStream);
        }
        stream.writeString(order.getOrderedClass().getName());
        stream.writeStrings(order.getOrderSpec());
        this.orderedWriter = order.orderedWriter(stream);
    }

    public FileOrderedWriter(String filename, Order<T> order) throws IOException {
        this(filename, order, true);
    }

    public FileOrderedWriter(File file, Order<T> order) throws IOException {
        this(file.getPath(), order, true);
    }

    public Class<T> getInputClass() {
        return order.getOrderedClass();
    }

    public void process(T object) throws IOException {
        orderedWriter.process(object);
    }

    public void close() throws IOException {
        orderedWriter.close();
        dataStream.close();
    }
}
