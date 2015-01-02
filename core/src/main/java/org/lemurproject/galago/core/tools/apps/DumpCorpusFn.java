/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

import java.io.PrintStream;
import java.util.Map;

/**
 *
 * @author sjh
 */
public class DumpCorpusFn extends AppFunction {

  @Override
  public String getName() {
    return "dump-corpus";
  }

  @Override
  public String getHelpString() {
    return "galago dump-corpus --path=<corpus> [limit fields]\n\n"
            + " Dumps all documents from a corpus file to stdout.\n"
            + " Limits (all boolean) include:\n pseudo tags terms metadata"
            + " text\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    CorpusReader reader = new CorpusReader(p.getString("path"));
    if (reader.getManifest().get("emptyIndexFile", false)) {
      output.println("Empty Corpus.");
      return;
    }

    DocumentReader.DocumentIterator iterator = reader.getIterator();
    DocumentComponents dc = new DocumentComponents(p);
    
    while (!iterator.isDone()) {
      output.println("#IDENTIFIER: " + iterator.getKeyString());
      Document document = iterator.getDocument(dc);
      output.println("#NAME: " + document.name);
      output.println("#METADATA");
      for (Map.Entry<String, String> entry : document.metadata.entrySet()) {
        output.println(entry.getKey() + "," + entry.getValue());
      }
      output.println("#TEXT");
      output.println(document.text);
      iterator.nextKey();
    }
    reader.close();
  }
}
