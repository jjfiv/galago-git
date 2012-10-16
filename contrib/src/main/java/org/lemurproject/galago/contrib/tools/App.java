/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;
import org.reflections.Reflections;

/**
 * This class will dynamically find Applications from the package:
 *
 * org.lemurproject.galago.contrib.tools.apps
 *
 * There is no need to add new applications to this file, just ensure there are
 * apps in the referenced package.
 *
 * @author sjh
 */
public class App {

  public final static Logger log;
  public final static HashMap<String, AppFunction> applications;

  // init function -- allows internal use of app function library
  static {
    log = Logger.getLogger("Galago");
    applications = new HashMap();
    Reflections reflections = new Reflections("org.lemurproject.galago");
    Set<Class<? extends AppFunction>> apps = reflections.getSubTypesOf(AppFunction.class);

    for (Class c : apps) {
      try {
        Constructor cons = c.getConstructor();
        AppFunction fn = (AppFunction) cons.newInstance();
        String name = fn.getName();

        // if we have a duplicated function - use the first one.
        if (applications.containsKey(fn.getName())) {
          log.log(Level.INFO, "Found duplicated function name: " + c.getName() + ". Arbitrarily using: " + applications.get(name).getClass().getName());
        } else {
          applications.put(fn.getName(), fn);
        }
      } catch (Exception e) {
        log.log(Level.INFO, "Failed to find constructor for app: {0}", c.getName());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    run(args);
  }

  public static void run(String[] args) throws Exception {
    run(args, System.out);
  }

  public static void run(String[] args, PrintStream out) throws Exception {
    // default to the 'help' function
    String fn = "help";

    // otherwise the first argument is the function name
    if (args.length > 0 && applications.containsKey(args[0])) {
      fn = args[0];
    }

    applications.get(fn).run(args, out);
  }

  public static void run(String fn, Parameters p, PrintStream out) throws Exception {
    applications.get(fn).run(p, out);
  }
}
