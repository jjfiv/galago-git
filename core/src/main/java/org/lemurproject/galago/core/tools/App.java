// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import org.lemurproject.galago.tupleflow.GalagoConf;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.reflections.Reflections;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

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
        appFunctions = new HashMap<>();

        // list of classpaths to scan
        List<String> cps = new ArrayList<String>();
        cps.add("org.lemurproject.galago");
        cps.add("org.lemurproject.galago.eval");

        Parameters p = GalagoConf.getAllOptions();
        if (p.isString("appclasspath") || p.isList("appclasspath", String.class)) {
            cps.addAll(p.getAsList("appclasspath", String.class));
        }

        for (String cp : cps) {
            processClassPath(cp);
        }
    }

    // MCZ 3/2015 - made this logic its own function so "wrapper" applications such
    // as Proteus can add AppFunctions by just calling this rather than having to
    // remember to add an "appclasspath" parameter to the galago config file.
    public static void processClassPath(String cp) {
        Reflections reflections = new Reflections(cp);
        Set<Class<? extends AppFunction>> apps = reflections.getSubTypesOf(AppFunction.class);

        for (Class<? extends AppFunction> c : apps) {
            try {
                Constructor<? extends AppFunction> cons = c.getConstructor();
                AppFunction fn = cons.newInstance();
                String name = fn.getName();

                // if we have a duplicated function - use the first one.
                if (appFunctions.containsKey(fn.getName())) {
                    log.info("Found duplicated function name: " + c.getName() + ". Arbitrarily using: " + appFunctions.get(name).getClass().getName());
                } else {
                    appFunctions.put(fn.getName(), fn);
                }
            } catch (Exception e) {
                log.log(Level.INFO, "Failed to find constructor for app: {0}", c.getName());
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
            log.log(Level.WARNING, "Could not find app: " + fn);
        }
    }
}
