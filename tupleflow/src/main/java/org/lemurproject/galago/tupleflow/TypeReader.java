// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public interface TypeReader<T> extends ExNihiloSource<T> {
    T read() throws IOException;

    void run() throws IOException;
}
