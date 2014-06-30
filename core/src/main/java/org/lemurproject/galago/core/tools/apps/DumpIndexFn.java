/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

import java.io.PrintStream;

/**
 *
 * @author sjh
 */
public class DumpIndexFn extends AppFunction {

  @Override
  public String getName() {
    return "dump-index";
  }

  @Override
  public String getHelpString() {
    return "galago dump-index <index-part>\n\n"
            + "  Dumps inverted list data from any index file in a StructuredIndex\n"
            + "  (That is, any index that has a readerClass that's a subclass of\n"
            + "  StructuredIndexPartReader).  Output is in CSV format.\n";
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

    // if we have a key-list index
    if (KeyListReader.class.isAssignableFrom(reader.getClass())) {
      while (!iterator.isDone()) {
        BaseIterator vIter = iterator.getValueIterator();
        ScoringContext sc = new ScoringContext();
        while (!vIter.isDone()) {
          sc.document = vIter.currentCandidate();
          output.println(vIter.getValueString(sc));
          vIter.movePast(vIter.currentCandidate());
        }
        iterator.nextKey();
      }

      // otherwise we could have a key-value index
    } else if (KeyValueReader.class.isAssignableFrom(reader.getClass())) {
      while (!iterator.isDone()) {
        output.println(iterator.getKeyString() + "," + iterator.getValueString());
        iterator.nextKey();
      }
    } else {
      output.println("Unable to read index as a key-list or a key-value reader.");
    }

    reader.close();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    String indexPath = p.getString("indexPath");
    run(new String[]{"", indexPath}, output);
  }
}
