/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.index.IndexPartModifier;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class DumpModifierFn extends AppFunction {

  @Override
  public String getName() {
    return "dump-modifier";
  }

  @Override
  public String getHelpString() {
    return "galago dump-modifier <modifier file>\n\n"
            + "  Dumps the contents of the specified modifier file.\n";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    if (args.length <= 1) {
      output.println(getHelpString());
      return;
    }

    IndexPartModifier modifier = DiskIndex.openIndexModifier(args[1]);

    if (modifier.getManifest().get("emptyIndexFile", false)) {
      output.println("Empty Index File.");
      return;
    }


    modifier.printContents(System.out);
    modifier.close();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String modifierPath = p.getString("modifierPath");
    run(new String[]{"", modifierPath}, output);
  }
}