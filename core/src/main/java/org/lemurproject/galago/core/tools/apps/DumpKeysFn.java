/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class DumpKeysFn extends AppFunction {

  @Override
  public String getName() {
    return "dump-keys";
  }

  @Override
  public String getHelpString() {
    return "galago dump-keys <index-part>\n\n"
            + "  Dumps keys from an index file.\n"
            + "  Output is in CSV format.\n";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    if (args.length <= 1) {
      output.println(getHelpString());
      return;
    }

    IndexPartReader reader = DiskIndex.openIndexPart(args[1]);

    if (reader.getManifest().get("emptyIndexFile", false)) {
      output.println("Empty Index File.");
      return;
    }

    KeyIterator iterator = reader.getIterator();
    while (!iterator.isDone()) {
      output.println(iterator.getKeyString());
      iterator.nextKey();
    }
    reader.close();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String indexPath = p.getString("indexPath");
    run(new String[]{"", indexPath}, output);
  }
}
