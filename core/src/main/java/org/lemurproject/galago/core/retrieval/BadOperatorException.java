// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval;

/**
 * An exception that is thrown when an unknown operator is encountered.
 *
 * @author irmarc
 */
public class BadOperatorException extends Exception {

  public BadOperatorException(String msg) {
    super(msg);
  }

  public BadOperatorException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public BadOperatorException(Throwable cause) {
    super(cause);
  }
}
