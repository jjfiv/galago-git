/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools;

import java.io.IOException;
import java.util.HashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 * Maps url based requests to the correct service
 *
 * [sjh] Pulled out of App.java
 * @author irmarc
 */
public class URLMappingHandler extends AbstractHandler {

  HashMap<String, Handler> handlers;
  Handler defaultHandler = null;

  public URLMappingHandler() {
    handlers = new HashMap<String, Handler>();
  }

  public void setHandler(String s, Handler h) {
    handlers.put(s, h);
  }

  public void setDefault(Handler h) {
    defaultHandler = h;
  }

  public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {
    String path = request.getPathInfo();
    Handler h = handlers.get(path);
    if (h != null) {
      h.handle(target, request, response, dispatch);
    } else if (defaultHandler != null) {
      defaultHandler.handle(target, request, response, dispatch);
    } else {
      throw new UnsupportedOperationException(" '" + path + "'  is not supported yet.");
    }
  }
}
