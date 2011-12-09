// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow;

import java.io.IOException;

/**
 * This is the base class for most TupleFlow steps.  Most TupleFlow steps convert
 * data from some type T to some type U, then call the process method on some other
 * processor.  This class abstracts the details of implementing that kind of class.
 * 
 * @author trevor
 */
public abstract class StandardStep<T, U> implements Processor<T>, Source<U> {
    public Processor<U> processor;

    public abstract void process(T object) throws IOException;
    public void setProcessor(Step next) throws IncompatibleProcessorException {
        Linkage.link(this, next);
    }
    public void close() throws IOException {
        processor.close();
    }
}
