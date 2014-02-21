/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools;

import java.io.IOException;
import java.util.HashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

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

  @Override
  public void handle(String target, Request jettyReq, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    jettyReq.setHandled(true);
    String path = request.getPathInfo();
    Handler h = handlers.get(path);
    if (h != null) {
      h.handle(target, jettyReq, request, response);
    } else if (defaultHandler != null) {
      defaultHandler.handle(target, jettyReq, request, response);
    } else {
      throw new UnsupportedOperationException(" '" + path + "'  is not supported yet.");
    }
  }
}
