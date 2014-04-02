package org.lemurproject.galago.contrib;

import org.lemurproject.galago.contrib.learning.LearnQueryParameters;
import org.lemurproject.galago.contrib.tools.*;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.PrintStream;
import java.util.logging.Logger;

/**
 * @author jfoley.
 */
public class Main {

    /**
     * function selection and processing
     */
    public final static Logger log;

    // init function -- allows internal use of app function library
    static {
      log = Logger.getLogger("Galago-App");

      AppFunction[] fns = {
          new LearnQueryParameters(),
          new BuildCollectionBackground(),
          new BuildFilteredCollection(),
          new BuildSketchIndex(),
          new BuildSpecialCollBackground(),
          new ExtractWikiAnchorText(),
          new GenerateWorkingSetQueries(),
          new TimedXCount()
      };

      for(AppFunction fn : fns) {
        App.registerAppFn(fn);
      }
    }

    public static void main(String[] args) throws Exception {
      App.run(args);
    }

    public static void run(String[] args) throws Exception {
      App.run(args);
    }

    public static void run(String[] args, PrintStream out) throws Exception {
      App.run(args, out);
    }

    public static void run(String fn, Parameters p, PrintStream out) throws Exception {
      App.run(fn, p, out);
    }
}
