/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class DumpDocNameFn extends AppFunction {

  @Override
  public String getName() {
    return "doc-name";
  }

  @Override
  public String getHelpString() {
    return "galago doc-name <names> <internal-number>\n"
            + "  Prints the external document identifier of the document <internal-number>.\n";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    if (args.length <= 2) {
      output.println(getHelpString());
      return;
    }
    String indexPath = args[1];
    String identifier = args[2];
    DiskNameReader reader = new DiskNameReader(indexPath);
    String docIdentifier = reader.getDocumentName(Integer.parseInt(identifier));
    reader.close();
    output.println(docIdentifier);
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String indexPath = p.getString("indexPath");
    String identifier = p.getString("identifier");
    run(new String[]{"", indexPath, identifier}, output);
  }
}
