// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Uses Java object serialization to answer requests over the 
 * wire. Doesn't generalize, but it's probably the fastest method.
 *
 * This handler does nothing but forward the remote java request to
 * the retrieval object attached to the Search object.
 *
 * @author irmarc
 */
public class StreamContextHandler extends AbstractHandler {

  Search search;

  public StreamContextHandler(Search search) {
    this.search = search;
  }

  @Override
  public void handle(String target, Request jettyReq, HttpServletRequest request,
          HttpServletResponse response) throws IOException, ServletException {
    jettyReq.setHandled(true);

    try {
      // Recover method
      ObjectInputStream ois = new ObjectInputStream(request.getInputStream());
      String methodName = ois.readUTF();

      // Get arguments
      int numArgs = (int) ois.readShort();
      Class argTypes[] = new Class[numArgs];

      for (int i = 0; i < numArgs; i++) {
        argTypes[i] = (Class) ois.readObject();
      }

      Object[] arguments = new Object[numArgs];
      for (int i = 0; i < numArgs; i++) {
        arguments[i] = ois.readObject();
      }

      ois.close();
      
      // NOW we can get the method itself and invoke it on our retrieval object
      // with the extracted arguments
      Method m = search.retrieval.getClass().getMethod(methodName, argTypes);
      Object result = m.invoke(search.getRetrieval(), arguments);

      // Finally send back our result
      ObjectOutputStream oos = new ObjectOutputStream(response.getOutputStream());
      oos.writeObject(result);
      response.flushBuffer();
    } catch (Exception e) {
      
      e.printStackTrace();
      System.err.println(e.toString());
      
      throw new RuntimeException(e);
    }
  }
}
