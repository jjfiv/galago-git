// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import org.lemurproject.galago.tupleflow.web.WebHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;

/**
 * Uses Java object serialization to answer requests over the 
 * wire. Doesn't generalize, but it's probably the fastest method.
 *
 * This handler does nothing but forward the remote java request to
 * the retrieval object attached to the Search object.
 *
 * @author irmarc
 * @see org.lemurproject.galago.core.retrieval.ProxyRetrieval
 */
public class StreamContextHandler implements WebHandler {

  Search search;

  public StreamContextHandler(Search search) {
    this.search = search;
  }

  @Override
  public void handle(HttpServletRequest request,
          HttpServletResponse response) throws IOException, ServletException {
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
        Method m = null;
        for (Method method : search.retrieval.getClass().getMethods()) {
            if (methodName.equals(method.getName()) && method.getParameterTypes().length == argTypes.length) {
                m = method;
                for (int i = 0; i < argTypes.length; i++) {
                    if (!method.getParameterTypes()[i].isAssignableFrom(argTypes[i])) {
                        m = null;
                    }
                }
                if (m != null) {
                    break;
                }
            }
        }
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
