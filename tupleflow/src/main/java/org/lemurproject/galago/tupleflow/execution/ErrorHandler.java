// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

/**
 *
 * @author trevor
 */
public interface ErrorHandler {
    public void addError(String errorString);
    public void addWarning(String warningString);
}
