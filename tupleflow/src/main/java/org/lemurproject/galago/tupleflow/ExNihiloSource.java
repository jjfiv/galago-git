// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public interface ExNihiloSource<T> extends Source<T> {
    public void run() throws IOException;
}
