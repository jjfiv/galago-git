package org.lemurproject.galago.tupleflow.web;

/**
 * @author jfoley.
 */
public class WebServerException extends Exception {
  public WebServerException(Exception ex) {
    super(ex);
  }
}
