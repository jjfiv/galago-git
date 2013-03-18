/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import java.util.Arrays;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.FileOrderedReader;
import org.lemurproject.galago.tupleflow.OrderedCombiner;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DumpConnectionFn extends AppFunction {

  @Override
  public String getName() {
    return "dump-connection";
  }

  @Override
  public String getHelpString() {
    return "galago dump-connection <connection-file>\n\n"
            + "  Dumps tuples from a Galago TupleFlow connection file in \n"
            + "  CSV format.  This can be useful for debugging strange problems \n"
            + "  in a TupleFlow execution.\n";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    if (args.length <= 1) {
      output.println(getHelpString());
      return;
    }

    args = Utility.subarray(args, 1);

    TypeReader reader;
    if (args.length == 1) {
      FileOrderedReader r = new FileOrderedReader(args[0]);
      System.err.println("COMPRESSION: " + r.getCompression());
      reader = r;
    } else {
      OrderedCombiner r = OrderedCombiner.combineFromFiles(Arrays.asList(args));
      System.err.println("COMPRESSION: " + r.getCompression());
      reader = r;
    }
    
    Object o;
    while ((o = reader.read()) != null) {
      output.println(o);
    }
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String connectionPath = p.getString("connectionPath");
    run(new String[]{"", connectionPath}, output);
  }
}
