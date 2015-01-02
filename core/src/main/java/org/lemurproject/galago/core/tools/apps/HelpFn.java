/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class HelpFn extends AppFunction {

  @Override
  public String getName() {
    return "help";
  }

  @Override
  public String getHelpString() {
    return "galago help [<function>]+\n\n"
            + "   Prints the usage information for any galago function.";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {

    StringBuilder defaultOutput = new StringBuilder(
            "Type 'galago help <command>' to get more help about any command.\n\n"
            + "Popular commands:\n"
            + "   build\n"
            + "   search\n"
            + "   batch-search\n\n"
            + "All commands:\n");
    List<String> cmds = new ArrayList(App.appFunctions.keySet());
    Collections.sort(cmds);
    for (String cmd : cmds) {
      defaultOutput.append("   ").append(cmd).append("\n");
    }

    // galago help
    if (args.length == 0) {
      output.println(defaultOutput);
      output.println();
    } else if (args.length == 1) {
      output.println(getHelpString());
      output.println();
      output.println(defaultOutput);
      output.println();
    } else {
      for (String arg : args) {
        if (!arg.equals("help")) {
          output.println("function: " + arg + "\n");
          if (App.appFunctions.containsKey(arg)) {
            output.println(App.appFunctions.get(arg).getHelpString());
          } else {
            output.println("  UNKNOWN.");
          }
          output.println();
        }
      }
    }
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    run((String[]) p.getList("function").toArray(new String[0]), output);
  }
}
