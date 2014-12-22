// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.utility.Parameters;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.*;

/**
 * Class for creating retrieval objects based on provided parameters
 *
 * options for retrieval creation: { "index" : "/path/to/index" }
 *
 * { "index" : [ "/path/to/index1", "/path/to/index2" ] }
 *
 * { "index" : { "g1" : "/path/to/index1", "g2" : "/path/to/index2" } }
 *
 * { "index" : { "g1" : [ "/path/to/index1", "/path/to/index2" ] "g2" :
 * "/path/to/index3" } }
 *
 *
 * @author irmarc, sjh
 */
public class RetrievalFactory {

  // Can't make one
  private RetrievalFactory() {
  }

  /** @deprecated use create instead! */
  @Deprecated
  public static Retrieval instance(Parameters parameters) throws Exception {
    return create(parameters);
  }

  public static Retrieval create(Parameters parameters) throws Exception {
    // if we have a single index:
    if (parameters.isString("index")) {
      return instance(parameters.getString("index"), parameters);

      // if we have a list of index paths:
    } else if (parameters.isList("index")) {
      List<String> indexes = parameters.getList("index", String.class);
      return instance(indexes, parameters);

      // if we have a mapping from groupName to list of index paths
    } else if (parameters.isMap("index")) {
      Parameters groups = parameters.getMap("index");
      Map<String, Retrieval> indexGroups = new HashMap<>();
      String defGroup = parameters.get("defaultGroup", groups.getKeys().iterator().next());

      for (String group : groups.getKeys()) {
        if (groups.isString(group)) {
          indexGroups.put(group, instance(groups.getString(group), parameters));
        } else if (groups.isList(group)) {
          indexGroups.put(group, instance(groups.getList(group, String.class), parameters));
        }
      }

      return new GroupRetrieval(indexGroups, parameters, defGroup);

      // otherwise we don't know what we have...
    } else {
      throw new RuntimeException("Could not open indexes from parameters :\n" + parameters.toString());
    }
  }

  /* get retrieval object
   * cases:
   *  1 index path - local
   *  1 index path - proxy
   *  many index paths - multi - locals
   *  many index paths - multi - proxies
   */
  public static Retrieval instance(String path, Parameters parameters) throws Exception {
    if (path.startsWith("http://")) {
      // create a proxy, using the ProxyRetrieval as the InvocationHandler
      InvocationHandler ih = new ProxyRetrieval(path, parameters);
      return (Retrieval) Proxy.newProxyInstance(Retrieval.class.getClassLoader(),
              new Class[]{Retrieval.class}, ih);
    } else {
      return new LocalRetrieval(path, parameters);
    }
  }
	
	public static Retrieval instance(String path) throws Exception {
		return instance(path, Parameters.create());
	}

  public static Retrieval instance(List<String> indexes, Parameters parameters) throws Exception {

    if (indexes.size() == 1) {
      return instance(indexes.get(0), parameters);
    }

    ArrayList<Thread> openers = new ArrayList<>();
    final Parameters shardParameters = parameters;
    final List<Retrieval> retrievals = Collections.synchronizedList(new ArrayList<Retrieval>());
    final List<String> errors = Collections.synchronizedList(new ArrayList<String>());

    for (final String path : indexes) {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            Retrieval r = instance(path, shardParameters);
            retrievals.add(r);
          } catch (Exception e) {
            System.err.println("Unable to load index (" + shardParameters.toString() + ") at path " + path + ": " + e.getMessage());
            e.printStackTrace(System.err);
            errors.add(e.toString());
          }
        }
      };

      t.start();
      openers.add(t);
    }

    for (Thread opener : openers) {
      opener.join();
    }

    if (!errors.isEmpty()) {
      throw new RuntimeException("Failed to open one or more indexes.");
    }

    return new MultiRetrieval(new ArrayList<>(retrievals), parameters);
  }
}
