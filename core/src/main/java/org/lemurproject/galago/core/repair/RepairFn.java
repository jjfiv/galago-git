package org.lemurproject.galago.core.repair;

import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

import java.io.PrintStream;

/**
 * @author jfoley.
 */
public class RepairFn extends AppFunction {

  @Override
  public String getName() {
    return "repair";
  }

  @Override
  public String getHelpString() {
    return "galago repair\n\n"+
      "  This app lets you repair pieces of your index.\n" +
      "  For now, only one 'repair action' is supported.\n" +
      "--action=(names-to-names-reverse)\n" +
      "--input=inputFiles\n" +
      "--output=outputFiles\n\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String action = p.getString("action");
    if(action.equals("names-to-names-reverse")) {
      IndexRepair.createNamesReverseFromNames(p.getString("input"), p.getString("output"), p);
    } else {
      throw new IllegalArgumentException("Action '"+action+"' on repair not supported yet.");
    }
  }

  public static void main(String[] args) throws Exception {
    App.main(new String[] {"repair",
      "--action=names-to-names-reverse",
      "--names=/home/jfoley/code/athena/indices/wiki-years.galago/names",
      "--output=/home/jfoley/Desktop/names.reverse.repaired"});
  }
}
