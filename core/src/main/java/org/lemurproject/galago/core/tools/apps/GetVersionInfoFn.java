/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.btree.format.BTreeFactory;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.VersionInfo;
/**
 *
 * @author smh
 */
public class GetVersionInfoFn extends AppFunction {

  @Override
  public String getName() {
    return "get-version-info";
  }

  @Override
  public String getHelpString() {
    return "galago get-version-info\n\n"
            + "  Displays the Galago version and version build date.";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    if (args.length > 1) {
      output.println (getHelpString());
      return;
    }

    VersionInfo.setGalagoVersionBuildAndIndexDateTime ();
    output.println ("Galago version: " + VersionInfo.getGalagoVersion());
    output.println ("Build date    : " + VersionInfo.getGalagoVersionBuildDateTime ());
  }


  @Override
  public void run (Parameters p, PrintStream output) throws Exception {
      run (new String[] {"get-version-info"}, output);
  }
    
}  //- end class GetVersionInfoFn
