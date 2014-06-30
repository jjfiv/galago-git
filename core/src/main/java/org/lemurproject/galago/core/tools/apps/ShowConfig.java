package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.GalagoConf;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.PrintStream;

/**
 * @author jfoley
 */
public class ShowConfig extends AppFunction {
  @Override
  public String getName() {
    return "show-config";
  }

  @Override
  public String getHelpString() {
    return "galago show-config <args> \n\n"
      + "  Spits information about the \".galago.conf\" file to stdout.\n";
  }

  /** We don't need any arguments. */
  @Override
  public boolean allowsZeroParameters() {
    return true;
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    output.println("###");
    for(File fp : GalagoConf.findGalagoConfigFiles()) {
      output.println("Found GalagoConf file: "+fp.getAbsolutePath());
    }
    output.println("###");
    output.println("Input GalagoConf files resolved to: ");
    output.println(GalagoConf.getAllOptions().toPrettyString());

    output.println("###");
    output.println("Temporary file roots: "+ FileUtility.getRoots());
  }
}
