/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class DumpDocIdFn extends AppFunction {

  @Override
  public String getName() {
    return "doc-id";
  }

  @Override
  public String getHelpString() {
    return "galago doc-id <names.reverse> <identifier>\n"
            + "  Prints the internal document number of the document named by <identifier>.\n";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    if (args.length <= 2) {
      output.println(getHelpString());
      return;
    }
    String indexPath = args[1];
    String identifier = args[2];
    DiskNameReverseReader reader = new DiskNameReverseReader(indexPath);
    long docNum = reader.getDocumentIdentifier(identifier);
    output.println(docNum);
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String indexPath = p.getString("indexPath");
    String identifier = p.getString("identifier");
    run(new String[]{"", indexPath, identifier}, output);
  }
}
