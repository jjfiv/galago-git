// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.tools;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 *
 * @author irmarc
 */
public class XMLContextHandler extends AbstractHandler {

  Search search;

  public XMLContextHandler(Search search) {
    this.search = search;
  }

  @Override
  public void handle(String target, HttpServletRequest request,
          HttpServletResponse response, int dispatch) throws IOException, ServletException {
    // we're not doing anything here yet - should work like the stream handler but produces
    // XML instead
  }
}
