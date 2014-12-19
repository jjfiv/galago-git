// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.IOException;
import java.util.Comparator;

/**
 *
 * @author trevor
 */
public class Unique<T> implements Processor<T> {
    Comparator<T> sameObject;
    T last;
    Processor<T> processor;

    /** Creates a new create of Unique */
    public Unique(Comparator<T> sameObject, Processor<T> processor) {
        this.sameObject = sameObject;
        this.last = null;
        this.processor = processor;
    }

    public void process(T object) throws IOException {
        if (last != null && 0 == sameObject.compare(object, last)) {
            return;
        }
        processor.process(object);
        last = object;
    }

    public void close() throws IOException {
        last = null;
        processor.close();
    }
}
