// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow;

import java.io.IOException;

public interface Processor<T> extends Step {
    public void process(T object) throws IOException;
    public void close() throws IOException;
}
