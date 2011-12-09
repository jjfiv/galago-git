// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

/**
 * An exception that is thrown when the submitted query is, for some reason,
 * not properly formulated.
 *
 * @author marc
 */
public class MalformedQueryException extends Exception {
    public MalformedQueryException(String message) {
        super(message);
    }
}
