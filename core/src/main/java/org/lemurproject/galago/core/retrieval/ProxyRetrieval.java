// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * This acts as the client-side stub for forwarding requests across 
 * a network connection to a remote index.
 *
 * The communication protocol uses Java Object Serialization to communicate
 * with remote Galago servers. See org.lemurproject.galago.core.tools.StreamContextHandler
 * for additional details.
 *
 * @author irmarc
 */
public class ProxyRetrieval implements InvocationHandler {

  String indexUrl;
  HashSet<String> unImplemented;
  // For async execution
  Thread queryRunner = null;
  Object[] argHolder;

  public ProxyRetrieval(String url, Parameters parameters) throws IOException {
    this.indexUrl = url + "/stream";
    unImplemented = new HashSet<String>();
  }

  public void close() throws IOException {
    // Nothing to do - index is serving remotely - possibly to several handlers
  }

  @Override
  public Object invoke(Object caller, Method method, Object[] args) throws Throwable {
    return invoke(method.getName(), args);
  }

  public Object invoke(String methodName, Object[] args) throws Throwable {

    // Check to make sure we shouldn't skip it
    if (unImplemented.contains(methodName)) {
      throw new UnsupportedOperationException("Proxy class does not support this operation.");
    }

    URL resource = new URL(this.indexUrl);
    HttpURLConnection connection = (HttpURLConnection) resource.openConnection();
    connection.setRequestMethod("GET");
    connection.setDoOutput(true);
    connection.setDoInput(true);
    connection.connect();

    // Write data directly to the stream
    OutputStream writeStream = connection.getOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(writeStream);

    // First the Method, which is not serializable directly
    oos.writeUTF(methodName);

    // Write length of arguments
    if (args == null) {
      args = new Object[0];
    }
    oos.writeShort((short) args.length);

    // Types of arguments
    for (int i = 0; i < args.length; i++) {
      oos.writeObject(args[i].getClass());
    }

    // Now write them out via serialization
    for (int i = 0; i < args.length; i++) {
      Object arg = args[i];
      oos.writeObject(arg);
    }
    oos.close();

    // Wait for response
    InputStream stream = connection.getInputStream();

    // Now get the response and re-instantiate
    // This requires that the return type is serializable
    ObjectInputStream ois = new ObjectInputStream(stream);
    Object response = ois.readObject();

    // Do we want to keep reconnecting and disconnecting?
    // Maybe a persistent connection is worth it?
    connection.disconnect();
    return response;
  }
}
