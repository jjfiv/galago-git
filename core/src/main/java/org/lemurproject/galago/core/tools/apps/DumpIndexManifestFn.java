/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

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
