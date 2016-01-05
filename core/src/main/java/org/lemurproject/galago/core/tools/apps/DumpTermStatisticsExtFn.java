/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.Arguments;

import java.io.PrintStream;
import java.util.Arrays;

/**
 * @author michaelz
 *         This is just a wrapper around DumpTermStatisticsFn to provide
 *         a bit more functionality and maintain backwards compatibility
 *         with the previous dump-term-stats.
 */
public class DumpTermStatisticsExtFn extends DumpTermStatisticsFn {

  @Override
  public String getName() {
    return "dump-term-stats-ext";
  }

  @Override
  public String getHelpString() {
    return "galago dump-term-stats-ext --indexParts=<index-part> --minTF=<n> --minDF=<n> \n\n"
            + "  Dumps <term> <frequency> <document count> statsistics from the"
            + " the specified index part.\n"
            + " Multiple index parts can be separated by commas.\n"
            + " minTF and minDF are optional.\n";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    Parameters p = Parameters.create();

    if (args.length < 2) {
      output.print(this.getHelpString());
      return;
    } else {
      p = Arguments.parse(Arrays.copyOfRange(args, 1, args.length));
    }

    String indexParts = p.getString("indexParts");
    Integer minTF = p.get("minTF", 0);
    Integer minDF = p.get("minDF", 0);

    execute(indexParts, minTF, minDF, output);
  }
}
