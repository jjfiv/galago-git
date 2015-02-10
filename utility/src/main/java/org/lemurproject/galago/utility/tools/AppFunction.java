/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.tools;

import org.lemurproject.galago.utility.Parameters;

import java.io.PrintStream;
import java.util.Arrays;

/**
 * General interface for all galago applications.
 *
 * @author sjh
 */
public abstract class AppFunction {

  /**
   * returns the name of the application.
   */
  public abstract String getName();

  /**
   * returns a string detailing the usage and parameters of the function.
   */
  public abstract String getHelpString();

  /**
   * run the function using the input parameters, using 'output' to print the
   * required output data.
   */
  public abstract void run(Parameters p, PrintStream output) throws Exception;


  /**
   * If this is false, we will show help if not given any parameters
   */
  public boolean allowsZeroParameters() {
    return false;
  }

  /**
   * run the function by processing command line parameters, using 'output' to
   * print the required output data.
   *
   * This function can be overwritten to allow alternative parameter input
   * methods.
   */
  public void run(String[] args, PrintStream output) throws Exception {
    Parameters p = Parameters.create();

    if (args.length == 1) {
      if(!allowsZeroParameters()) {
        output.print(this.getHelpString());
        return;
      }
    } else if (args.length > 1) {
      p = Arguments.parse(Arrays.copyOfRange(args, 1, args.length));
      // don't want to wipe an existing parameter:
    }

    run(p, output);
  }

  /** Simple help string builder */
  public String makeHelpStr(String... kv) {
    if(kv.length % 2 != 0) {
      throw new IllegalArgumentException("Expected even number of key,value pairs!");
    }
    StringBuilder out = new StringBuilder();
    out.append(this.getName()).append("\n\n");
    for(int i=0; i<kv.length; i+=2) {
      out.append("\t--").append(kv[i])
        .append('=').append(kv[i+1]).append("\n");
    }
    return out.toString();
  }


}
