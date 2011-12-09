// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

/**
 *
 * @author trevor
 */
public class IncompatibleProcessorException extends Exception {
    public IncompatibleProcessorException(String message) {
        super(message);
    }

    public IncompatibleProcessorException(String message, Throwable e) {
        super(message, e);
    }
}
