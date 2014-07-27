// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow;

import java.io.Closeable;
import java.io.IOException;

public interface Processor<T> extends Step, Closeable {
    public void process(T object) throws IOException;
    public void close() throws IOException;
}
