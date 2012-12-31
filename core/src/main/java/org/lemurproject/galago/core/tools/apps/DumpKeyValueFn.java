/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DumpKeyValueFn extends AppFunction {

  @Override
  public String getName() {
    return "dump-key-value";
  }

  @Override
  public String getHelpString() {
    return "galago dump-key-value <indexwriter-file> <key>\n\n"
            + "  Dumps all data associated with a particular key from a file\n"
            + "  created by IndexWriter.  This includes corpus files and all\n"
            + "  index files built by Galago.\n";
  }

  @Override
  public void run(String[] args, PrintStream output) throws Exception {
    if (args.length <= 2) {
      output.println(getHelpString());
      return;
    }
    String key = args[2];
    output.printf("Dumping key: %s\n", key);
    IndexPartReader reader = DiskIndex.openIndexPart(args[1]);

    if (reader.getManifest().get("emptyIndexFile", false)) {
      output.println("Empty Index File.");
      return;
    }

    KeyIterator iterator = reader.getIterator();

    if (iterator.skipToKey(Utility.fromString(key))) {
      if (KeyListReader.class.isAssignableFrom(reader.getClass())) {	
        ValueIterator vIter = iterator.getValueIterator();
	ScoringContext context = new ScoringContext();
        while (!vIter.isDone()) {
	  context.document = vIter.currentCandidate();
          output.printf("%s\n", vIter.getEntry());
          vIter.movePast(vIter.currentCandidate());
        }
      } else {
        output.printf("%s\n", iterator.getValueString());
      }
    }
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String indexPath = p.getString("indexPath");
    String key = p.getString("key");
    run(new String[]{"", indexPath, key}, output);
  }
}