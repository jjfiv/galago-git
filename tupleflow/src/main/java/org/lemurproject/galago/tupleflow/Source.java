// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;

/**
 * An object that can generate objects of type T
 * @author trevor
 */
public interface Source<T> extends Step {
    public void setProcessor(Step processor) throws IncompatibleProcessorException;
}
