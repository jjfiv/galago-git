/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.btree.format.BTreeFactory;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class DumpIndexManifestFn extends AppFunction {

  @Override
  public String getName() {
    return "dump-index-manifest";
  }

  @Override
  public String getHelpString() {
    return "galago dump-index-manifest <index-path>\n\n"
            + "  Dumps the manifest for an index file.";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    if (args.length <= 1) {
      output.println(getHelpString());
      return;
    }

    BTreeReader indexReader = BTreeFactory.getBTreeReader(args[1]);
    output.println(indexReader.getManifest().toPrettyString());
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String indexPath = p.getString("indexPath");
    run(new String[]{"", indexPath}, output);
  }
}
