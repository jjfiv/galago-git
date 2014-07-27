// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.IOException;
import java.util.List;

/**
 * @author trevor
 */
public class Multi<T> implements Processor<T> {
    Processor[] processors;

    /**
     * Creates a new instance of Multi
     */
    public Multi(Processor[] processors) {
        this.processors = processors;
    }

    public Multi(List<Processor> processors) {
        this(processors.toArray(new Processor[processors.size()]));
    }

    @SuppressWarnings("unchecked")
    public void process(T object) throws IOException {
        for (Processor processor : processors) {
            processor.process(object);
        }
    }

    public void close() throws IOException {
        for (Processor processor : processors) {
            processor.close();
        }
    }
}
