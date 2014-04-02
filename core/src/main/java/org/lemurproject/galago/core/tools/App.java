// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import org.lemurproject.galago.core.eval.Eval;
import org.lemurproject.galago.core.index.merge.MergeIndex;
import org.lemurproject.galago.core.tools.apps.*;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.PrintStream;
import java.util.HashMap;
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
    appFunctions = new HashMap<String,AppFunction>();

    AppFunction[] fns = {
        new BatchSearch(),
        new BuildIndex(),
        new BuildPartialIndex(),
        new BuildSpecialPart(),
        new BuildStemmerConflation(),
        new BuildWindowIndex(),
        new ChainFns(),
        new DocCountFn(),
        new DumpConnectionFn(),
        new DumpCorpusFn(),
        new DumpDocFn(),
        new DumpDocIdFn(),
        new DumpDocNameFn(),
        new DumpIndexFn(),
        new DumpIndexManifestFn(),
        new DumpKeysFn(),
        new DumpKeyValueFn(),
        new DumpNamesLengths(),
        new DumpTermStatisticsFn(),
        new HarvestLinksFn(),
        new HelpFn(),
        new OverwriteManifestFn(),
        new PageRankFn(),
        new SearchFn(),
        new StatsFn(),
        new ThreadedBatchSearch(),
        new TimedBatchSearch(),
        new TransformQueryFn(),
        new XCountFn(),
        new MakeCorpus(),
        new Eval(),
        new MergeIndex(),
    };

    for(AppFunction fn : fns) {
      registerAppFn(fn);
    }
  }

  public static void registerAppFn(AppFunction fn) {
    appFunctions.put(fn.getName(), fn);
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
      log.warning("Could not find app: " + fn);
    }
  }
}
