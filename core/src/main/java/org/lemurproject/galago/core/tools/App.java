// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.reflections.Reflections;

/**
 * @author sjh, irmarc, trevor
 */
public class App {

  /**
   * function selection and processing
   */
  public final static Logger log;
  public final static HashMap<String, AppFunction> appFunctions;

  // init function -- allows internal use of app function library
  static {
    log = Logger.getLogger("Galago-App");
    appFunctions = new HashMap();

    // list of classpaths to scan
    List<String> cps = new ArrayList();
    cps.add("org.lemurproject.galago");

    Parameters p = Utility.getAllOptions();
    if (p.isString("appclasspath")
            || p.isList("appclasspath", Parameters.Type.STRING)) {
      cps.addAll((List<String>) p.getAsList("appclasspath"));
    }

    for (String cp : cps) {
      Reflections reflections = new Reflections(cp);
      Set<Class<? extends AppFunction>> apps = reflections.getSubTypesOf(AppFunction.class);

      for (Class c : apps) {
        try {
          Constructor cons = c.getConstructor();
          AppFunction fn = (AppFunction) cons.newInstance();
          String name = fn.getName();

          // if we have a duplicated function - use the first one.
          if (appFunctions.containsKey(fn.getName())) {
            log.log(Level.INFO, "Found duplicated function name: " + c.getName() + ". Arbitrarily using: " + appFunctions.get(name).getClass().getName());
          } else {
            appFunctions.put(fn.getName(), fn);
          }
        } catch (Exception e) {
          log.log(Level.INFO, "Failed to find constructor for app: {0}", c.getName());
        }
      }
    }
  }

  /*
   * Eval function
   */
  public static void main(String[] args) throws Exception {
    App.run(args);
  }

  public static void run(String[] args) throws Exception {
    run(args, System.out);
  }

  public static void run(String[] args, PrintStream out) throws Exception {
    String fn = "help";

    if (args.length > 0 && appFunctions.containsKey(args[0])) {
      fn = args[0];
    }
    appFunctions.get(fn).run(args, out);
  }

  public static void run(String fn, Parameters p, PrintStream out) throws Exception {
    if (appFunctions.containsKey(fn)) {
      appFunctions.get(fn).run(p, out);
    } else {
      log.severe("Could not find app: " + fn);
    }
  }
}