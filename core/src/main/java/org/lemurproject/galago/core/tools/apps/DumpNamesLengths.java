/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.index.NamesReader.NamesIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class DumpNamesLengths extends AppFunction {

  @Override
  public String getName() {
    return "dump-name-length";
  }

  @Override
  public String getHelpString() {
    return "galago dump-name-length --index=[indexPath]\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("index")) {
      output.println(this.getHelpString());
    }

    DiskIndex index = new DiskIndex(p.getString("index"));

    NamesIterator namesItr = index.getNamesIterator();
    LengthsIterator lengthsItr = index.getLengthsIterator();
    
    ScoringContext sc = new ScoringContext();
    namesItr.setContext(sc);
    lengthsItr.setContext(sc);
    
    while (!namesItr.isDone()) {
      int docId = namesItr.currentCandidate();
      
      sc.document = docId;
      
      String docName = namesItr.getCurrentName();

      lengthsItr.syncTo(docId);
      int docLen = 0;
      if (lengthsItr.currentCandidate() == docId) {
        docLen = lengthsItr.length();
      }

      if ((docLen == 0) && p.get("zeros", true)) {
        output.println(docId + "\t" + docName + "\t" + docLen);
      } else if (p.get("non-zeros", true)) {
        output.println(docId + "\t" + docName + "\t" + docLen);
      }
      namesItr.movePast(docId);
    }
  }
}
